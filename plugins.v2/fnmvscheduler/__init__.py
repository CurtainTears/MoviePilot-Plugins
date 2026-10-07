import time
import os
import threading
import json
import random
import re
import requests
from typing import Any, List, Dict, Tuple, Optional
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.helper.mediaserver import MediaServerHelper
from app.plugins import _PluginBase
from app.schemas import ServiceInfo, MediaServerLibrary, MediaServerConf, TransferInfo
from app.core.context import MediaInfo
from app.schemas.types import EventType
from app.schemas.file import FileItem
from app.log import logger, log_settings
from app.schemas import NotificationType

# 导入 fnapi
import app.modules.trimemedia.api as fnapi
from app.utils.url import UrlUtils



class Fnmvscheduler(_PluginBase):
    # 插件名称
    plugin_name = "飞牛影视调度器"
    # 插件描述
    plugin_desc = "根据平台整理通告，按设置的模式智能触发飞牛影视媒体库扫描。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/EWEDLCM/MoviePilot-Plugins/main/icons/fnmv.png"
    # 插件版本
    plugin_version = "2.2.3"
    # 插件作者
    plugin_author = "EWEDL"
    # 作者主页
    author_url = "https://github.com/EWEDLCM/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "fnmvscheduler_"
    # 加载顺序
    plugin_order = 1
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _enabled = False
    _run_once = False
    _check_tasks_once = False
    _scan_rules: str = ""
    _selected_mediaservers: List[str] = []
    _scan_lock = threading.Lock()
    _task_scheduler: Optional[BackgroundScheduler] = None
    _clear_log_once = False 
    _auto_clear_log = False 
    _cron_schedule: str = "0 8 * * 1" 
    _precision_scan_enabled = False 
    _signature_manager = None 
    _library_scan_requests = {} 
    _token_manager = None 
    _precision_scan_notify = False 
    _precision_scan_msgtype = None 
    _path_mapping: str = ""

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled", False)
            self._run_once = config.get("run_once", False)
            self._scan_rules = config.get("scan_rules", "")
            self._check_tasks_once = config.get("check_tasks_once", False)
            self._selected_mediaservers = config.get("selected_mediaservers", [])
            self._clear_log_once = config.get("clear_log_once", False)
            self._auto_clear_log = config.get("auto_clear_log", False)
            self._cron_schedule = config.get("cron_schedule", "0 8 * * 1") # 从配置中读取Cron表达式
            self._precision_scan_enabled = config.get("precision_scan_enabled", False)
            self._precision_scan_notify = config.get("precision_scan_notify", False)
            self._precision_scan_msgtype = config.get("precision_scan_msgtype")
            self._path_mapping = config.get("path_mapping", "")

        self._task_scheduler = BackgroundScheduler(timezone=settings.TZ)

        # 初始化签名管理器和Token管理器
        self._signature_manager = SignatureManager()
        self._token_manager = Fnmvscheduler.TokenManager()

        if self._enabled and self._run_once:
            logger.info("【飞牛影视调度器】检测到 '媒体库获取测试' 选项已勾选...")
            run_once_scheduler = BackgroundScheduler(timezone=settings.TZ)
            run_once_scheduler.add_job(
                func=self._execute_and_reset,
                trigger=DateTrigger(run_date=datetime.now() + timedelta(seconds=3)),
                name="Log Media Libraries Once"
            )
            run_once_scheduler.start()

        if self._enabled and self._check_tasks_once:
            logger.info("【飞牛影视调度器】检测到 '获取扫描队列测试' 选项已勾选，准备执行一次性任务检查...")
            check_scheduler = BackgroundScheduler(timezone=settings.TZ)
            check_scheduler.add_job(
                func=self._execute_check_and_reset,
                trigger=DateTrigger(run_date=datetime.now() + timedelta(seconds=3)),
                name="Check Running Tasks Once"
            )
            check_scheduler.start()

        if self._enabled and self._clear_log_once:
            logger.info("【飞牛影视调度器】检测到 '清除日志' 选项已勾选，准备执行一次性日志清空...")
            clear_log_scheduler = BackgroundScheduler(timezone=settings.TZ)
            clear_log_scheduler.add_job(
                func=self._execute_clear_log_and_reset,
                trigger=DateTrigger(run_date=datetime.now() + timedelta(seconds=3)),
                name="Clear Log Once"
            )
            clear_log_scheduler.start()

        
        # 注意：日志清理定时任务现在通过 get_service() 方法向 MoviePilot 框架注册
        # 框架会统一管理定时任务，不再需要手动调度


        logger.info("【飞牛影视调度器】插件初始化状态：")
        logger.info(f"  - 插件启用: {self._enabled}")
        logger.info(f"  - 媒体库获取测试 (run_once): {self._run_once}")
        logger.info(f"  - 获取扫描队列测试 (check_tasks_once): {self._check_tasks_once}")
        logger.info(f"  - 一次性清除日志 (clear_log_once): {self._clear_log_once}")
        logger.info(f"  - 自动清除日志 (auto_clear_log): {self._auto_clear_log}")
        logger.info(f"  - 定期日志Cron表达式: {self._cron_schedule}")
        logger.info(f"  - 定义的扫描规则: {self._parse_scan_rules()}")
        logger.info(f"  - 生效的媒体服务器: {self._selected_mediaservers if self._selected_mediaservers else '所有'}")
        logger.info(f"  - 精确扫描功能: {self._precision_scan_enabled}")
        logger.info(f"  - 开启通知: {self._precision_scan_notify}")
        logger.info(f"  - 通知渠道: {self._precision_scan_msgtype or 'SiteMessage'}")


    def _execute_clear_log_cron(self):
        """
        执行Cron定时清除日志任务。
        """
        try:
            logger.info("【飞牛影视调度器】执行Cron定时清除日志任务...")
            # 清空主日志文件
            main_log_file_path = log_settings.LOG_PATH / "plugins" / "fnmvscheduler.log"
            if main_log_file_path.exists():
                with open(main_log_file_path, 'w', encoding='utf-8') as f:
                    f.truncate(0)
                logger.info(f"【飞牛影视调度器】主日志文件 '{main_log_file_path}' 已清空。")
            else:
                logger.warning(f"【飞牛影视调度器】主日志文件 '{main_log_file_path}' 不存在，无需清空。")

            # 删除备份日志文件
            log_dir = log_settings.LOG_PATH / "plugins"
            if log_dir.exists() and log_dir.is_dir():
                for file in os.listdir(log_dir):
                    if file.startswith("fnmvscheduler.log.") and file.endswith(tuple(str(i) for i in range(10))): # 匹配 .N 结尾的备份文件
                        backup_file_path = log_dir / file
                        os.remove(backup_file_path)
                        logger.info(f"【飞牛影视调度器】已删除备份日志文件: '{backup_file_path}'")
            
        except Exception as e:
            logger.error(f"【飞牛影视调度器】执行Cron定时清除日志时发生错误: {e}")

    def clear_log_service(self):
        """
        专门用于MoviePilot框架定时服务的日志清理方法。
        这个方法只执行日志清理，不包含其他逻辑。
        """
        try:
            # 清空主日志文件
            main_log_file_path = log_settings.LOG_PATH / "plugins" / "fnmvscheduler.log"
            if main_log_file_path.exists():
                with open(main_log_file_path, 'w', encoding='utf-8') as f:
                    f.truncate(0)
                logger.info(f"【飞牛影视调度器】主日志文件 '{main_log_file_path}' 已清空。")
            else:
                logger.warning(f"【飞牛影视调度器】主日志文件 '{main_log_file_path}' 不存在，无需清空。")

            # 删除备份日志文件
            log_dir = log_settings.LOG_PATH / "plugins"
            if log_dir.exists() and log_dir.is_dir():
                deleted_count = 0
                for file in os.listdir(log_dir):
                    if file.startswith("fnmvscheduler.log.") and file.endswith(tuple(str(i) for i in range(10))): # 匹配 .N 结尾的备份文件
                        backup_file_path = log_dir / file
                        os.remove(backup_file_path)
                        deleted_count += 1
                
                if deleted_count > 0:
                    logger.info(f"【飞牛影视调度器】已删除 {deleted_count} 个备份日志文件。")
            
            logger.info("【飞牛影视调度器】定时日志清理任务完成。")
            
        except Exception as e:
            logger.error(f"【飞牛影视调度器】执行定时日志清理时发生错误: {e}")

    def get_service(self) -> List[Dict[str, Any]]:
        """
        向 MoviePilot 框架注册定时服务。
        只注册日志清理服务，确保定时任务只执行日志清理。
        """
        if self._enabled and self._auto_clear_log and self._cron_schedule:
            try:
                if str(self._cron_schedule).strip().count(" ") == 4:
                    logger.info(f"【飞牛影视调度器】向框架注册日志清理定时服务，Cron表达式: '{self._cron_schedule}'")
                    return [{
                        "id": "fnmvscheduler_clear_log",
                        "name": "飞牛影视调度器日志清理服务",
                        "trigger": CronTrigger.from_crontab(self._cron_schedule, timezone=settings.TZ),
                        "func": self.clear_log_service,
                        "kwargs": {}
                    }]
                else:
                    logger.error(f"【飞牛影视调度器】Cron表达式 '{self._cron_schedule}' 格式不正确，定时服务注册失败。")
            except Exception as e:
                logger.error(f"【飞牛影视调度器】注册定时服务失败：{e}", exc_info=True)
        return []


    def _log_plugin_status_recurring(self):
        """
        定期记录插件状态的日志任务。
        """
        logger.info("【飞牛影视调度器】定时任务：插件当前状态概览：")
        logger.info(f"  - 插件启用: {self._enabled}")
        logger.info(f"  - 媒体库获取测试 (run_once): {self._run_once}")
        logger.info(f"  - 获取扫描队列测试 (check_tasks_once): {self._check_tasks_once}")
        logger.info(f"  - 一次性清除日志 (clear_log_once): {self._clear_log_once}")
        logger.info(f"  - 自动清除日志 (auto_clear_log): {self._auto_clear_log}")
        logger.info(f"  - 定期日志Cron表达式: {self._cron_schedule}")
        logger.info(f"  - 定义的扫描规则: {self._parse_scan_rules()}")
        logger.info(f"  - 生效的媒体服务器: {self._selected_mediaservers if self._selected_mediaservers else '所有'}")


    def _execute_clear_log_and_reset(self):
        """
        执行一次性的“清除日志”并自动关闭开关。
        """
        try:
            # 清空主日志文件
            main_log_file_path = log_settings.LOG_PATH / "plugins" / "fnmvscheduler.log"
            if main_log_file_path.exists():
                with open(main_log_file_path, 'w', encoding='utf-8') as f:
                    f.truncate(0)
                logger.info(f"【飞牛影视调度器】主日志文件 '{main_log_file_path}' 已清空。")
            else:
                logger.warning(f"【飞牛影视调度器】主日志文件 '{main_log_file_path}' 不存在，无需清空。")

            # 删除备份日志文件
            log_dir = log_settings.LOG_PATH / "plugins"
            if log_dir.exists() and log_dir.is_dir():
                for file in os.listdir(log_dir):
                    if file.startswith("fnmvscheduler.log.") and file.endswith(tuple(str(i) for i in range(10))): # 匹配 .N 结尾的备份文件
                        backup_file_path = log_dir / file
                        os.remove(backup_file_path)
                        logger.info(f"【飞牛影视调度器】已删除备份日志文件: '{backup_file_path}'")
            
        except Exception as e:
            logger.error(f"【飞牛影视调度器】执行清除日志时发生错误: {e}")
        finally:
            self._clear_log_once = False
            self.update_config({
                "enabled": self._enabled,
                "run_once": self._run_once,
                "scan_rules": self._scan_rules,
                "selected_mediaservers": self._selected_mediaservers,
                "check_tasks_once": self._check_tasks_once,
                "clear_log_once": False,
                "auto_clear_log": self._auto_clear_log,
                "cron_schedule": self._cron_schedule,
                "precision_scan_enabled": self._precision_scan_enabled,
                "precision_scan_notify": self._precision_scan_notify,
                "precision_scan_msgtype": self._precision_scan_msgtype,
                "path_mapping": self._path_mapping
                            })
            logger.info("【飞牛影视调度器】'清除日志' 选项已重置为 False。")


    def _ensure_scheduler_running(self):
        """
        一个辅助方法，确保调度器在需要时已经启动。
        """
        if self._task_scheduler and not self._task_scheduler.running:
            self._task_scheduler.start()
            logger.info("【飞牛影视调度器】事件触发，按需启动内部任务调度器。")

    def _parse_scan_rules(self) -> List[Dict[str, str]]:
        """解析用户输入的扫描规则"""
        rules = []
        if not self._scan_rules:
            return rules
            
        lines = self._scan_rules.strip().split('\n')
        for i, line in enumerate(lines):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
                
            parts = line.split('#')
            if len(parts) == 3:
                path = parts[0].strip()
                library_name = parts[1].strip()
                mode = parts[2].strip()
                
                if path and library_name and mode in ['本地', '网盘']:
                    rules.append({
                        'path': path,
                        'library_name': library_name,
                        'mode': mode
                    })
                else:
                    logger.warning(f"【飞牛影视调度器】规则格式错误，已跳过第 {i+1} 行: {line}")
            else:
                logger.warning(f"【飞牛影视调度器】规则格式错误（需要3个'#'分隔的部分），已跳过第 {i+1} 行: {line}")
        
        return rules

    def _execute_check_and_reset(self):
        """
        执行一次性的“获取扫描队列测试”并自动关闭开关。
        """
        try:
            logger.info("【飞牛影视调度器】开始检查所有已配置飞牛服务器的正在运行任务...")
            mediaserver_helper = MediaServerHelper()
            all_configs = mediaserver_helper.get_configs()
            
            checked_any = False
            for config in all_configs.values():
                if config.type != 'trimemedia':
                    continue
                
                checked_any = True
                logger.info(f"--- 正在检查服务器: {config.name} ---")
                host = config.config.get("host")
                username = config.config.get("username")
                password = config.config.get("password")

                # 使用TokenManager获取token
                token, api, base_url = self._token_manager.get_token(host, username, password)

                if token:
                    tasks = self._get_running_tasks(api, base_url, token)
                    logger.info(f"【飞牛影视调度器】服务器 '{config.name}' 的运行中任务GUID列表: {tasks if tasks else '当前无任务'}")
                    api.close()
                else:
                    logger.warning(f"【飞牛影视调度器】无法登录服务器 '{config.name}'，跳过检查。")
                    if api: api.close()
            
            if not checked_any:
                logger.warning("【飞牛影视调度器】未找到任何已配置的飞牛(trimemedia)媒体服务器以进行检查。")

        except Exception as e:
            logger.error(f"【飞牛影视调度器】执行获取扫描队列测试时发生错误: {e}")
        finally:
            logger.info("【飞牛影视调度器】'获取扫描队列测试' 任务完成，选项已自动重置为 False。")
            self._check_tasks_once = False
            self.update_config({
                "enabled": self._enabled,
                "run_once": self._run_once,
                "scan_rules": self._scan_rules,
                "selected_mediaservers": self._selected_mediaservers,
                "check_tasks_once": False,
                "clear_log_once": self._clear_log_once,
                "auto_clear_log": self._auto_clear_log,
                "cron_schedule": self._cron_schedule,
                "precision_scan_enabled": self._precision_scan_enabled,
                "precision_scan_notify": self._precision_scan_notify,
                "precision_scan_msgtype": self._precision_scan_msgtype,
                "path_mapping": self._path_mapping
            })

    
  
    def _get_media_libraries(self, api: fnapi.Api, base_url: str, token: str) -> List[Dict[str, Any]]:
        """
        获取媒体库列表
        """
        try:
            response = api.request("/mdb")
            if response and response.success:
                return response.data or []
            else:
                logger.warning(f"【飞牛影视调度器】获取媒体库列表时API返回错误: {response}")
        except Exception as e:
            logger.error(f"【飞牛影视调度器】获取媒体库列表时发生错误: {e}")
        return []

    def _scan_folder(self, api: fnapi.Api, base_url: str, token: str, library_id: str, folder_paths: List[str]) -> bool:
        """
        执行文件夹扫描
        """
        try:
            scan_url = f"{base_url.rstrip('/')}/api/v1/mdb/scan/{library_id}"
            payload = {"dir_list": folder_paths}

            logger.info(f"【飞牛影视调度器】正在发起文件夹扫描请求，URL: {scan_url}")
            logger.info(f"【飞牛影视调度器】扫描文件夹: {folder_paths}")

            response = api.request(f"/mdb/scan/{library_id}", method="post", data=payload)
            if response and response.success:
                logger.info(f"【飞牛影视调度器】文件夹扫描请求发送成功")
                return True
            else:
                logger.warning(f"【飞牛影视调度器】文件夹扫描请求失败: {response}")
                return False
        except Exception as e:
            logger.error(f"【飞牛影视调度器】执行文件夹扫描时发生错误: {e}")
            return False

    def _execute_and_reset(self):
        """
        执行记录逻辑并重置 '运行一次' 标志
        """
        try:
            self._log_media_libraries()
        except Exception as e:
            logger.error(f"【飞牛影视调度器】执行记录时发生错误: {e}")
        finally:
            self._run_once = False
            self.update_config({
                "enabled": self._enabled,
                "run_once": self._run_once,
                "scan_rules": self._scan_rules,
                "check_tasks_once": self._check_tasks_once,
                "selected_mediaservers": self._selected_mediaservers,
                "clear_log_once": self._clear_log_once,
                "auto_clear_log": self._auto_clear_log,
                "cron_schedule": self._cron_schedule,
                "precision_scan_enabled": self._precision_scan_enabled,
                "precision_scan_notify": self._precision_scan_notify,
                "precision_scan_msgtype": self._precision_scan_msgtype,
                "path_mapping": self._path_mapping
                            })
            logger.info("【飞牛影视调度器】'媒体库获取测试' 选项已重置为 False。")

    
    def _get_running_tasks(self, api: fnapi.Api, base_url: str, token: str) -> List[str]:
        """通过宿主公开接口获取扫描任务，兼容不含私有会话的API实现。"""
        try:
            task_url = f"{base_url.rstrip('/')}/api/v1/task/running"
            logger.debug(f"【飞牛影视调度器】正在请求运行中的任务列表，URL: {task_url}")
            response = api.request("/task/running")
            if response and response.success:
                tasks = response.data or []
                running_guids = list(set(task.get("guid") for task in tasks if task.get("guid")))
                logger.debug(f"【飞牛影视调度器】当前正在运行的扫描任务GUID: {running_guids if running_guids else '无'}")
                return running_guids
            else:
                logger.warning(f"【飞牛影视调度器】获取任务列表时API返回错误: {response}")
        except Exception as e:
            logger.error(f"【飞牛影视调度器】获取正在运行的任务列表时发生网络或解析错误: {e}")
        return []

    @eventmanager.register(EventType.TransferComplete)
    def handle_transfer_complete(self, event: Event):
        if not self._enabled: return
        
        logger.debug(f"【飞牛影视调度器】收到原始转移完成事件: {event.event_data}")

        self._ensure_scheduler_running()
        
        if not self._scan_lock.acquire(blocking=False):
            logger.debug("【飞牛影视调度器】事件处理中，跳过重复触发。")
            return
        try:
            transfer_info: Optional[TransferInfo] = event.event_data.get("transferinfo")
            target_diritem: Optional[FileItem] = getattr(transfer_info, 'target_diritem', None)
            target_item = getattr(transfer_info, 'target_item', None)

            if not (target_item and hasattr(target_item, 'path') and target_item.path): return

            logger.info(f"【飞牛影视调度器】接收到转移完成事件，完整文件路径: {target_item.path}")

            if not isinstance(target_item.path, (str, Path)): return

            # 提取倒数第二级目录路径
            try:
                original_file_path = str(target_item.path)
                path_parts = original_file_path.strip('/').split('/')

                if len(path_parts) < 2:
                    normalized_target_path = os.path.normpath(original_file_path)
                else:
                    # 取倒数第二级及之前的所有部分
                    second_level_parent_parts = path_parts[:-1]
                    extracted_path = '/' + '/'.join(second_level_parent_parts) + '/'
                    normalized_target_path = os.path.normpath(extracted_path)

                    logger.debug(f"【飞牛影视调度器】路径提取: 原始文件路径 '{original_file_path}' -> 识别目录 '{extracted_path}'")

            except Exception as e:
                logger.warning(f"【飞牛影视调度器】提取倒数第二级路径时出错: {e}，使用原始路径")
                normalized_target_path = os.path.normpath(str(target_item.path))

            scan_rules = self._parse_scan_rules()
            if not scan_rules:
                logger.debug("【飞牛影视调度器】未配置任何扫描规则，跳过处理。")
                return

            mediaserver_helper = MediaServerHelper()
            all_services = mediaserver_helper.get_services()
            all_configs = mediaserver_helper.get_configs()
            if not all_services: return

            matched = False
            for rule in scan_rules:
                normalized_rule_path = os.path.normpath(rule['path'])
                
                if normalized_target_path.startswith(normalized_rule_path):
                    logger.info(f"【飞牛影视调度器】路径 '{normalized_target_path}' 匹配到规则: {rule['path']} -> {rule['library_name']} ({rule['mode']}模式)")
                    
                    found_lib, found_config = None, None
                    for name, service_info in all_services.items():
                        if str(service_info.type) != 'trimemedia': continue
                        
                        libraries = service_info.instance.get_librarys()
                        for lib in libraries:
                            if lib.name == rule['library_name']:
                                found_lib = lib
                                found_config = all_configs.get(name)
                                break
                        if found_lib: break
                            
                    if found_lib and found_config:
                        # 优先使用精确扫描（如果启用）
                        if self._precision_scan_enabled:
                            logger.debug(f"【飞牛影视调度器】精确扫描已启用，使用精确扫描模式处理媒体库扫描")
                            self._handle_precision_scan_request(found_lib, found_config, normalized_target_path)
                        elif rule['mode'] == '网盘':
                            self._handle_cloud_scan_request(found_lib, found_config)
                        elif rule['mode'] == '本地':
                            self._handle_local_scan_request(found_lib, found_config)
                        else:
                            logger.warning(f"【飞牛影视调度器】规则中定义了未知的模式: '{rule['mode']}'")

                        matched = True
                        break
                    else:
                        logger.warning(f"【飞牛影视调度器】规则匹配成功，但在已配置的飞牛服务器中未找到名为 '{rule['library_name']}' 的媒体库。")

            if not matched:
                logger.info(f"【飞牛影视调度器】路径 '{normalized_target_path}' 未匹配到任何扫描规则。")
        finally:
            self._scan_lock.release()

    def _handle_local_scan_request(self, lib: MediaServerLibrary, feiniu_config: MediaServerConf):
        debounce_job_id = f"debounce_scan_local_{lib.id}"
        run_time = datetime.now() + timedelta(minutes=1)

        if self._task_scheduler.get_job(debounce_job_id):
            self._task_scheduler.reschedule_job(debounce_job_id, trigger='date', run_date=run_time)
            logger.info(f"【飞牛影视调度器-本地模式】检测到媒体库 '{lib.name}' 的新请求，重置1分钟防抖计时器。")
        else:
            logger.info(f"【飞牛影视调度器-本地模式】媒体库 '{lib.name}' 收到扫描请求，启动1分钟防抖等待。")
            self._task_scheduler.add_job(
                self._after_local_debounce, 'date', run_date=run_time,
                args=[lib, feiniu_config], id=debounce_job_id, name=f"Debounce Local Scan for {lib.name}", replace_existing=True)

    def _get_total_pending_paths_count(self) -> int:
        """获取所有媒体库的待扫描路径总数"""
        return sum(req.get_path_count() for req in self._library_scan_requests.values())

    def _handle_precision_scan_request(self, lib: MediaServerLibrary, feiniu_config: MediaServerConf, folder_path: str):
        """
        处理精确扫描请求，支持多路径收集和去重，带2分钟防抖、3分钟重试循环和2分钟静默等待
        """
        # 获取或创建媒体库扫描请求管理器
        if lib.id not in self._library_scan_requests:
            self._library_scan_requests[lib.id] = Fnmvscheduler.LibraryScanRequest(lib.id, lib.name)

        library_request = self._library_scan_requests[lib.id]

        # 添加路径到请求管理器
        is_new_path = library_request.add_path(folder_path)

        if is_new_path:
            current_lib_count = library_request.get_path_count()
            total_count = self._get_total_pending_paths_count()
            logger.info(f"【飞牛影视调度器-精确扫描】媒体库 '{lib.name}' 添加新的扫描路径: {folder_path}")
            logger.info(f"【飞牛影视调度器-精确扫描】待扫描路径: 本库 {current_lib_count} 个，全部 {total_count} 个")
        else:
            logger.debug(f"【飞牛影视调度器-精确扫描】媒体库 '{lib.name}' 收到重复的扫描路径: {folder_path}")

        # 任务ID
        debounce_job_id = library_request.debounce_job_id
        retry_job_id = f"retry_precision_scan_{lib.id}"
        final_scan_job_id = f"final_precision_scan_{lib.id}"

        # 移除可能存在的重试检查任务
        try:
            if self._task_scheduler.get_job(retry_job_id):
                self._task_scheduler.remove_job(retry_job_id)
                logger.info(f"【飞牛影视调度器-精确扫描】检测到媒体库 '{lib.name}' 的新事件，中断了正在进行的3分钟重试检查。")
        except JobLookupError: pass

        # 移除可能存在的静默等待任务
        try:
            if self._task_scheduler.get_job(final_scan_job_id):
                self._task_scheduler.remove_job(final_scan_job_id)
                logger.info(f"【飞牛影视调度器-精确扫描】检测到媒体库 '{lib.name}' 的新事件，中断了正在进行的2分钟静默等待。")
        except JobLookupError: pass

        # 创建或重置防抖任务
        run_time = datetime.now() + timedelta(minutes=2)
        current_lib_count = library_request.get_path_count()
        total_count = self._get_total_pending_paths_count()

        if self._task_scheduler.get_job(debounce_job_id):
            self._task_scheduler.reschedule_job(debounce_job_id, trigger='date', run_date=run_time)
            logger.info(f"【飞牛影视调度器-精确扫描】媒体库 '{lib.name}' 收到新路径，重置2分钟防抖计时器（本库 {current_lib_count} 个，全部 {total_count} 个）")
        else:
            logger.info(f"【飞牛影视调度器-精确扫描】媒体库 '{lib.name}' 收到精确扫描请求，启动2分钟防抖等待（本库 {current_lib_count} 个，全部 {total_count} 个）")
            self._task_scheduler.add_job(
                self._after_precision_debounce_check, 'date', run_date=run_time,
                args=[lib, feiniu_config], id=debounce_job_id, name=f"Debounce Precision Scan for {lib.name}", replace_existing=True)

    def _after_precision_debounce_check(self, lib: MediaServerLibrary, feiniu_config: MediaServerConf):
        """
        精确扫描2分钟防抖结束后的检查逻辑
        """
        logger.info(f"【飞牛影视调度器-精确扫描】媒体库 '{lib.name}' 的2分钟防抖期结束，开始检查当前扫描状态...")

        host = feiniu_config.config.get("host")
        username = feiniu_config.config.get("username")
        password = feiniu_config.config.get("password")

        # 使用TokenManager获取token
        token, api, base_url = self._token_manager.get_token(host, username, password)

        if not token:
            logger.error(f"【飞牛影视调度器-精确扫描】无法登录飞牛服务器 '{feiniu_config.name}'，扫描任务中止。")
            return

        is_scanning = False
        try:
            if lib.id in self._get_running_tasks(api, base_url, token):
                is_scanning = True
        finally:
            api.close()

        if is_scanning:
            logger.info(f"【飞牛影视调度器-精确扫描】检测到媒体库 '{lib.name}' 正在扫描中。启动3分钟后的重试检查。")
            retry_job_id = f"retry_precision_scan_{lib.id}"
            self._task_scheduler.add_job(
                self._retry_precision_scan_loop, 'date', run_date=datetime.now() + timedelta(minutes=3),
                args=[lib, feiniu_config], id=retry_job_id, name=f"Retry Precision Scan for {lib.name}", replace_existing=True)
        else:
            logger.info(f"【飞牛影视调度器-精确扫描】媒体库 '{lib.name}' 当前无扫描任务。")
            self._execute_precision_scan(lib, feiniu_config)

    def _retry_precision_scan_loop(self, lib: MediaServerLibrary, feiniu_config: MediaServerConf):
        """
        精确扫描3分钟重试检查循环
        """
        logger.info(f"【飞牛影视调度器-精确扫描】正在对媒体库 '{lib.name}' 进行3分钟后的重试检查...")

        host = feiniu_config.config.get("host")
        username = feiniu_config.config.get("username")
        password = feiniu_config.config.get("password")

        # 使用TokenManager获取token
        token, api, base_url = self._token_manager.get_token(host, username, password)

        if not token:
            logger.error(f"【飞牛影视调度器-精确扫描】无法登录飞牛服务器 '{feiniu_config.name}'，重试检查中止。")
            return

        is_scanning = False
        try:
            if lib.id in self._get_running_tasks(api, base_url, token):
                is_scanning = True
        finally:
            api.close()

        if is_scanning:
            logger.info(f"【飞牛影视调度器-精确扫描】媒体库 '{lib.name}' 仍在扫描中。将在3分钟后再次检查。")
            retry_job_id = f"retry_precision_scan_{lib.id}"
            self._task_scheduler.add_job(
                self._retry_precision_scan_loop, 'date', run_date=datetime.now() + timedelta(minutes=3),
                args=[lib, feiniu_config], id=retry_job_id, name=f"Retry Precision Scan for {lib.name}", replace_existing=True)
        else:
            logger.info(f"【飞牛影视调度器-精确扫描】确认媒体库 '{lib.name}' 的扫描任务已结束。开始2分钟的静默等待期。")
            final_scan_job_id = f"final_precision_scan_{lib.id}"
            self._task_scheduler.add_job(
                self._execute_precision_scan, 'date', run_date=datetime.now() + timedelta(minutes=2),
                args=[lib, feiniu_config], id=final_scan_job_id, name=f"Final Precision Scan for {lib.name}", replace_existing=True)

    def _after_local_debounce(self, lib: MediaServerLibrary, feiniu_config: MediaServerConf):
        logger.info(f"【飞牛影视调度器-本地模式】媒体库 '{lib.name}' 的1分钟防抖期结束，立即执行扫描。")
        self._execute_scan(lib, feiniu_config)

    def _execute_precision_scan(self, lib: MediaServerLibrary, feiniu_config: MediaServerConf):
        """
        执行精确扫描，支持多路径分别处理
        使用新的签名算法和重试机制，对每个路径单独发送扫描请求
        """
        # 获取该媒体库的所有待扫描路径
        if lib.id not in self._library_scan_requests:
            logger.warning(f"【飞牛影视调度器-精确扫描】媒体库 '{lib.name}' 没有待扫描路径")
            return

        library_request = self._library_scan_requests[lib.id]
        pending_paths = library_request.get_all_paths()

        if not pending_paths:
            logger.warning(f"【飞牛影视调度器-精确扫描】媒体库 '{lib.name}' 没有待扫描路径")
            return

        logger.info(f"【飞牛影视调度器-精确扫描】开始对媒体库 '{lib.name}' 执行精确扫描")
        logger.info(f"【飞牛影视调度器-精确扫描】待扫描路径数量: {len(pending_paths)}")

        # 打印所有待扫描路径
        for i, path_request in enumerate(pending_paths, 1):
            logger.info(f"  {i}. {path_request.folder_path}")

        # 获取API连接信息
        host = feiniu_config.config.get("host")
        username = feiniu_config.config.get("username")
        password = feiniu_config.config.get("password")

        # 使用TokenManager获取token
        logger.debug(f"【飞牛影视调度器-精确扫描】正在获取认证token，主机: {host}")
        token, api, base_url = self._token_manager.get_token(host, username, password)

        if not token:
            logger.error(f"【飞牛影视调度器-精确扫描】无法获取有效的token，精确扫描中止")
            return
        logger.debug(f"【飞牛影视调度器-精确扫描】token获取成功")

        # 对每个路径分别执行精确扫描（串行处理，避免Task duplicate错误）
        successful_paths = []
        failed_paths = []

        for i, path_request in enumerate(pending_paths, 1):
            folder_path = path_request.folder_path
            logger.info(f"【飞牛影视调度器-精确扫描】=== 处理路径 {i}/{len(pending_paths)} ===")
            logger.info(f"【飞牛影视调度器-精确扫描】路径: {folder_path}")

            # 第一个路径不需要等待，后续路径需要等待前一个完成
            if i > 1:
                logger.info(f"【飞牛影视调度器-精确扫描】等待前一个扫描任务完成...")
                if not self._wait_for_scan_completion(api, base_url, token, lib):
                    logger.warning(f"【飞牛影视调度器-精确扫描】等待前一个扫描任务完成超时，跳过当前路径")
                    failed_paths.append(folder_path)
                    logger.warning(f"【飞牛影视调度器-精确扫描】❌ 路径扫描失败: {folder_path} (等待超时)")
                    continue

                # 任务完成后，额外等待30秒缓冲时间
                logger.info(f"【飞牛影视调度器-精确扫描】前一个扫描任务已完成，额外等待30秒缓冲时间...")
                time.sleep(30)

            # 双重保险：检查当前是否有运行中的任务
            try:
                running_tasks = self._get_running_tasks(api, base_url, token)
                if lib.id in running_tasks:
                    logger.info(f"【飞牛影视调度器-精确扫描】检测到媒体库 '{lib.name}' 仍有运行中的扫描任务，继续等待...")

                    # 等待循环，直到任务完成或收到签名错误
                    scan_success = False
                    while True:
                        time.sleep(30)  # 等待30秒

                        # 再次检查任务状态
                        running_tasks = self._get_running_tasks(api, base_url, token)
                        if lib.id not in running_tasks:
                            logger.info(f"【飞牛影视调度器-精确扫描】媒体库 '{lib.name}' 任务已完成，开始扫描")
                            break

                        logger.debug(f"【飞牛影视调度器-精确扫描】媒体库 '{lib.name}' 仍在运行中，继续等待...")

                    # 任务完成后，执行精确扫描
                    scan_success = self._scan_single_path_with_precision(
                        api, base_url, token, lib, feiniu_config, folder_path
                    )
                else:
                    # 没有任务冲突，直接执行扫描
                    scan_success = self._scan_single_path_with_precision(
                        api, base_url, token, lib, feiniu_config, folder_path
                    )
            except Exception as e:
                logger.warning(f"【飞牛影视调度器-精确扫描】检查运行任务时发生错误: {e}")
                scan_success = False

            if scan_success:
                successful_paths.append(folder_path)
                logger.info(f"【飞牛影视调度器-精确扫描】✅ 路径扫描成功: {folder_path}")
                # 不需要固定等待，直接进入下一个路径的等待检查
            else:
                failed_paths.append(folder_path)
                logger.warning(f"【飞牛影视调度器-精确扫描】❌ 路径扫描失败: {folder_path}")

        # 关闭API连接
        api.close()

        # 统计结果
        logger.info(f"【飞牛影视调度器-精确扫描】媒体库 '{lib.name}' 精确扫描完成")
        logger.info(f"  - 成功: {len(successful_paths)} 个路径")
        logger.info(f"  - 失败: {len(failed_paths)} 个路径")

        # 发送通知（只在有失败路径时）
        if failed_paths:
            self._send_precision_scan_notification(successful_paths, failed_paths)

        # 清理已处理的请求
        del self._library_scan_requests[lib.id]

    def _scan_single_path_with_precision(self, api, base_url: str, token: str, lib: MediaServerLibrary,
                                       feiniu_config: MediaServerConf, folder_path: str) -> bool:
        """
        对单个路径执行精确扫描
        返回是否成功
        """
        try:
            # 构建精确扫描请求参数
            scan_url = f"{base_url.rstrip('/')}/api/v1/mdb/scan/{lib.id}"
            api_path = f"/mdb/scan/{lib.id}"
            # [自定义修改] 动态读取UI配置的路径替换规则
            feiniu_path = folder_path
            if self._path_mapping:
                mapping_lines = self._path_mapping.strip().split('\n')
                for line in mapping_lines:
                    line = line.strip()
                    if line and '|' in line:
                        old_path, new_path = line.split('|', 1)
                    if old_path.strip() and new_path.strip():
                        feiniu_path = feiniu_path.replace(old_path.strip(), new_path.strip())
        
            payload = {"dir_list": [feiniu_path]}
            logger.debug(f"【飞牛影视调度器-精确扫描】准备发起精确扫描请求:")
            logger.debug(f"  - URL: {scan_url}")
            logger.debug(f"  - API路径: {api_path}")
            logger.debug(f"  - 文件夹: {folder_path}")

            # 由宿主对实际发送的请求体统一签名，避免依赖API内部会话和重复序列化。
            response = api.request(api_path, method="post", data=payload)
            if response and response.success:
                logger.info(f"【飞牛影视调度器-精确扫描】🎉 请求成功！")
                return True
            else:
                logger.warning(f"【飞牛影视调度器-精确扫描】请求失败: {response}")
                return False

        except Exception as e:
            logger.warning(f"【飞牛影视调度器-精确扫描】请求异常: {type(e).__name__}: {e}")
            return False

    def _wait_for_scan_completion(self, api, base_url: str, token: str, lib: MediaServerLibrary, max_wait_minutes: int = 10) -> bool:
        """
        等待扫描任务完成，20秒检查一次

        :param api: API实例
        :param base_url: 基础URL
        :param token: 认证令牌
        :param lib: 媒体库信息
        :param max_wait_minutes: 最大等待时间（分钟）
        :return: 是否成功等待到任务完成
        """
        start_time = time.time()
        max_wait_seconds = max_wait_minutes * 60

        logger.info(f"【飞牛影视调度器-精确扫描】开始等待媒体库 '{lib.name}' 的扫描任务完成...")

        while time.time() - start_time < max_wait_seconds:
            try:
                running_tasks = self._get_running_tasks(api, base_url, token)
                if lib.id not in running_tasks:
                    logger.info(f"【飞牛影视调度器-精确扫描】媒体库 '{lib.name}' 扫描任务已完成，可以继续下一个路径")
                    return True

                elapsed_time = int(time.time() - start_time)
                logger.debug(f"【飞牛影视调度器-精确扫描】媒体库 '{lib.name}' 仍在扫描中，已等待 {elapsed_time} 秒，20秒后再次检查...")
                time.sleep(20)  # 20秒检查一次

            except Exception as e:
                logger.warning(f"【飞牛影视调度器-精确扫描】检查扫描状态时发生错误: {e}")
                time.sleep(20)  # 出错时也等待20秒再重试

        logger.warning(f"【飞牛影视调度器-精确扫描】等待媒体库 '{lib.name}' 扫描完成超时（{max_wait_minutes}分钟）")
        return False

    def _handle_cloud_scan_request(self, lib: MediaServerLibrary, feiniu_config: MediaServerConf):
        retry_job_id = f"retry_check_{lib.id}"
        final_scan_job_id = f"final_scan_{lib.id}"
        try:
            if self._task_scheduler.get_job(retry_job_id):
                self._task_scheduler.remove_job(retry_job_id)
                logger.info(f"【飞牛影视调度器-网盘模式】媒体库 '{lib.name}' 收到新事件，中断了正在进行的3分钟重试检查。")
        except JobLookupError: pass
        try:
            if self._task_scheduler.get_job(final_scan_job_id):
                self._task_scheduler.remove_job(final_scan_job_id)
                logger.info(f"【飞牛影视调度器-网盘模式】媒体库 '{lib.name}' 收到新事件，中断了正在进行的10分钟静默等待。")
        except JobLookupError: pass

        debounce_job_id = f"debounce_scan_cloud_{lib.id}"
        run_time = datetime.now() + timedelta(minutes=5)
        if self._task_scheduler.get_job(debounce_job_id):
            self._task_scheduler.reschedule_job(debounce_job_id, trigger='date', run_date=run_time)
            logger.info(f"【飞牛影视调度器-网盘模式】检测到媒体库 '{lib.name}' 的新请求，重置5分钟防抖计时器。")
        else:
            logger.info(f"【飞牛影视调度器-网盘模式】媒体库 '{lib.name}' 收到扫描请求，启动5分钟防抖等待。")
            self._task_scheduler.add_job(
                self._after_debounce_check, 'date', run_date=run_time, 
                args=[lib, feiniu_config], id=debounce_job_id, name=f"Debounce Cloud Check for {lib.name}", replace_existing=True)

    def _after_debounce_check(self, lib: MediaServerLibrary, feiniu_config: MediaServerConf):
        logger.info(f"【飞牛影视调度器-网盘模式】媒体库 '{lib.name}' 的5分钟防抖期结束，开始检查当前扫描状态...")
        host = feiniu_config.config.get("host")
        username = feiniu_config.config.get("username")
        password = feiniu_config.config.get("password")

        # 使用TokenManager获取token
        token, api, base_url = self._token_manager.get_token(host, username, password)

        if not token:
            logger.error(f"【飞牛影视调度器】无法登录飞牛服务器 '{feiniu_config.name}'，扫描任务中止。")
            return
        is_scanning = False
        try:
            if lib.id in self._get_running_tasks(api, base_url, token): is_scanning = True
        finally: api.close()
        if is_scanning:
            logger.info(f"【飞牛影视调度器-网盘模式】检测到媒体库 '{lib.name}' 正在扫描中。启动3分钟后的重试检查。")
            retry_job_id = f"retry_check_{lib.id}"
            self._task_scheduler.add_job(
                self._retry_check_loop, 'date', run_date=datetime.now() + timedelta(minutes=3), 
                args=[lib, feiniu_config], id=retry_job_id, name=f"Retry Check for {lib.name}", replace_existing=True)
        else:
            logger.info(f"【飞牛影视调度器-网盘模式】确认媒体库 '{lib.name}' 当前无扫描任务，立即执行扫描。")
            self._execute_scan(lib, feiniu_config)

    def _retry_check_loop(self, lib: MediaServerLibrary, feiniu_config: MediaServerConf):
        logger.info(f"【飞牛影视调度器-网盘模式】正在对媒体库 '{lib.name}' 进行3分钟后的重试检查...")
        host, username, password = feiniu_config.config.get("host"), feiniu_config.config.get("username"), feiniu_config.config.get("password")

        # 使用TokenManager获取token
        token, api, base_url = self._token_manager.get_token(host, username, password)

        if not token:
            logger.error(f"【飞牛影视调度器】无法登录飞牛服务器 '{feiniu_config.name}'，重试检查中止。")
            return
        is_scanning = False
        try:
            if lib.id in self._get_running_tasks(api, base_url, token): is_scanning = True
        finally: api.close()
        if is_scanning:
            logger.info(f"【飞牛影视调度器-网盘模式】媒体库 '{lib.name}' 仍在扫描中。将在3分钟后再次检查。")
            retry_job_id = f"retry_check_{lib.id}"
            self._task_scheduler.add_job(
                self._retry_check_loop, 'date', run_date=datetime.now() + timedelta(minutes=3), 
                args=[lib, feiniu_config], id=retry_job_id, name=f"Retry Check for {lib.name}", replace_existing=True)
        else:
            logger.info(f"【飞牛影视调度器-网盘模式】确认媒体库 '{lib.name}' 的扫描任务已结束。开始10分钟的静默等待期。")
            final_scan_job_id = f"final_scan_{lib.id}"
            self._task_scheduler.add_job(
                self._execute_scan, 'date', run_date=datetime.now() + timedelta(minutes=10), 
                args=[lib, feiniu_config], id=final_scan_job_id, name=f"Final Scan for {lib.name}", replace_existing=True)

    def _execute_scan(self, lib: MediaServerLibrary, feiniu_config: MediaServerConf):
        logger.info(f"【飞牛影视调度器】开始对媒体库 '{lib.name}' (ID: {lib.id}) 发起扫描请求...")
        host, username, password = feiniu_config.config.get("host"), feiniu_config.config.get("username"), feiniu_config.config.get("password")

        # 使用TokenManager获取token
        token, api, _ = self._token_manager.get_token(host, username, password)

        if not token:
            logger.error(f"【飞牛影视调度器】执行扫描前登录飞牛服务器 '{feiniu_config.name}' 失败。")
            return
        try:
            mdb_to_scan = fnapi.MediaDb(guid=lib.id, category=fnapi.Category.MOVIE, name=lib.name)
            success = api.mdb_scan(mdb_to_scan)
            logger.info(f"【飞牛影视调度器】媒体库 '{lib.name}' 扫描请求发送结果：{'成功' if success else '失败'}。")

            if success:
                monitor_job_id = f"monitor_scan_{lib.id}"
                run_time = datetime.now() + timedelta(seconds=30)
                logger.info(f"【飞牛影视调度器】将在30秒后开始监控 '{lib.name}' 的扫描状态。")
                self._task_scheduler.add_job(
                    self._monitor_scan_completion, 'date', run_date=run_time,
                    args=[lib, feiniu_config], id=monitor_job_id, name=f"Monitor Scan for {lib.name}", replace_existing=True)
        except Exception as e:
            logger.error(f"【飞牛影视调度器】执行扫描时发生意外错误: {e}")
        finally:
            api.close()
            
    def _monitor_scan_completion(self, lib: MediaServerLibrary, feiniu_config: MediaServerConf):
        logger.debug(f"【飞牛影视调度器】正在检查媒体库 '{lib.name}' 的扫描状态...")
        host, username, password = feiniu_config.config.get("host"), feiniu_config.config.get("username"), feiniu_config.config.get("password")

        # 使用TokenManager获取token
        token, api, base_url = self._token_manager.get_token(host, username, password)

        if not token:
            logger.warning(f"【飞牛影视调度器】无法登录服务器 '{feiniu_config.name}'，暂时跳过状态检查。将在30秒后重试。")
            monitor_job_id = f"monitor_scan_{lib.id}"
            run_time = datetime.now() + timedelta(seconds=30)
            self._task_scheduler.add_job(
                self._monitor_scan_completion, 'date', run_date=run_time,
                args=[lib, feiniu_config], id=monitor_job_id, name=f"Monitor Scan for {lib.name}", replace_existing=True)
            return

        is_still_scanning = False
        try:
            running_tasks = self._get_running_tasks(api, base_url, token)
            if lib.id in running_tasks:
                is_still_scanning = True
        finally:
            api.close()
            
        if is_still_scanning:
            logger.debug(f"【飞牛影视调度器】媒体库 '{lib.name}' 仍在扫描中，将在30秒后再次检查。")
            monitor_job_id = f"monitor_scan_{lib.id}"
            run_time = datetime.now() + timedelta(seconds=30)
            self._task_scheduler.add_job(
                self._monitor_scan_completion, 'date', run_date=run_time,
                args=[lib, feiniu_config], id=monitor_job_id, name=f"Monitor Scan for {lib.name}", replace_existing=True)
        else:
            logger.info(f"【{lib.name}】已完成扫描任务")

    def _log_media_libraries(self):
        logger.info("【飞牛影视调度器】开始获取媒体库信息...")
        mediaserver_helper = MediaServerHelper()
        if not mediaserver_helper:
            logger.error("【飞牛影视调度器】MediaServerHelper 未初始化。无法获取媒体库信息。")
            return
        all_services: Optional[Dict[str, ServiceInfo]] = mediaserver_helper.get_services()
        if not all_services:
            logger.warning("【飞牛影视调度器】未找到任何配置的媒体服务器。")
            return
        logged_any = False
        for name, service_info in all_services.items():
            if self._selected_mediaservers and name not in self._selected_mediaservers: continue
            logger.info(f"【飞牛影视调度器】正在处理媒体服务器: {name} (类型: {str(service_info.type)})")
            if service_info.instance.is_inactive():
                logger.warning(f"【飞牛影视调度器】媒体服务器 {name} 未连接或不活跃，跳过。")
                continue
            try:
                libraries: List[MediaServerLibrary] = service_info.instance.get_librarys()
                if not libraries:
                    logger.info(f"【飞牛影视调度器】媒体服务器 {name} 未发现任何媒体库。")
                    continue
                for lib in libraries:
                    logged_any = True
                    logger.info(f"【飞牛影视调度器】媒体服务器: {name}, 库ID: {lib.id}, 库类型: {str(lib.type)}, 库名称: {lib.name}, 路径: {lib.path}")
                    logger.info("-" * 20)
            except Exception as e:
                logger.error(f"【飞牛影视调度器】获取媒体服务器 {name} 的媒体库信息时发生错误: {e}")
        if not logged_any: logger.info("【飞牛影视调度器】没有媒体库信息被记录到日志中。")
        logger.info("【飞牛影视调度器】媒体库信息记录完成。")

    def get_state(self) -> bool:
        return self._enabled

    def get_command(self) -> List[Dict[str, Any]]: pass
    def get_api(self) -> List[Dict[str, Any]]: pass
    def get_page(self) -> List[dict]: pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        mediaserver_helper = MediaServerHelper()
        all_services_configs = mediaserver_helper.get_configs()
        select_items = [{"title": config.name, "value": config.name} for config in all_services_configs.values()]
        # 编历 NotificationType 枚举，生成消息类型选项
        MsgTypeOptions = []
        for item in NotificationType:
            MsgTypeOptions.append({
                "title": item.value,
                "value": item.name
            })
        
        form_config = [
            {
                "component": "VCard", "props": {"variant": "outlined", "class": "mb-3"},
                "content": [
                    {
                        "component": "VTabs", "props": {"model": "tab", "grow": True, "color": "primary"},
                        "content": [
                            {
                                "component": "VTab", "props": {"value": "tab-basic"},
                                "content": [{"component": "VIcon", "props": {"icon": "mdi-cog", "start": True, "color": "#1976D2"}}, {"component": "span", "text": "基础设置"}]
                            },
                            {
                                "component": "VTab", "props": {"value": "tab-advanced"},
                                "content": [{"component": "VIcon", "props": {"icon": "mdi-cog-outline", "start": True, "color": "#9C27B0"}}, {"component": "span", "text": "高级"}]
                            },
                            {
                                "component": "VTab", "props": {"value": "tab-maintenance"},
                                "content": [{"component": "VIcon", "props": {"icon": "mdi-tools", "start": True, "color": "#FF9800"}}, {"component": "span", "text": "维护"}]
                            },
                        ]
                    },
                    {"component": "VDivider"},
                    {
                        "component": "VWindow", "props": {"model": "tab"},
                        "content": [
                            {
                                "component": "VWindowItem", "props": {"value": "tab-basic"},
                                "content": [
                                    {"component": "VCardText", "content": [
                                        {
                                            "component": "VRow",
                                            "content": [
                                                {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                                            ]
                                        },
                                        {
                                            "component": "VTextarea",
                                            "props": {
                                                "model": "scan_rules",
                                                "label": "扫描规则",
                                                "placeholder": "整理后路径#媒体库名称#模式(本地/网盘)",
                                                "hint": "每行一条规则，例如：/volume1/media/电视剧#电视剧#本地",
                                                "persistent-hint": True,
                                                "rows": 5,
                                                "class": "mt-4"
                                            }
                                        },
                                        {
                                            "component": "VSelect",
                                            "props": {
                                                "multiple": True, "chips": True, "clearable": True,
                                                "model": "selected_mediaservers",
                                                "label": "选择媒体服务器（留空则处理所有）",
                                                "items": select_items,
                                                "class": "mt-4"
                                            }
                                        }
                                    ]}
                                ]
                            },
                            {
                                "component": "VWindowItem", "props": {"value": "tab-advanced"},
                                "content": [
                                    {"component": "VCardText", "content": [
                                        {
                                            "component": "VRow",
                                            "content": [
                                                {"component": "VCol", "props": {"cols": 12}, "content": [
                                                    {"component": "VCard", "props": {"variant": "outlined", "class": "mb-3"}, "content": [
                                                        {"component": "VCardTitle", "props": {"class": "text-subtitle-1"}, "content": [{"component": "span", "text": "精确扫描设置"}]},
                                                        {"component": "VDivider"},
                                                        {"component": "VCardText", "content": [
                                                            {
                                                                "component": "VRow",
                                                                "content": [
                                                                    {"component": "VCol", "props": {"cols": 12}, "content": [
                                                                        {"component": "VSwitch", "props": {"model": "precision_scan_enabled", "label": "精确扫描", "color": "primary"}}
                                                                    ]}
                                                                ]
                                                            },
                                                            {
                                                                "component": "VRow",
                                                                "content": [
                                                                    {
                                                                        "component": "VCol", 
                                                                        "props": {"cols": 12}, 
                                                                        "content": [
                                                                            {
                                                                                "component": "VTextarea", 
                                                                                "props": {
                                                                                    "model": "path_mapping",
                                                                                    "label": "路径替换映射 (解决SMB/Docker路径不一致)",
                                                                                    "placeholder": "/NAS/|/vol02/1000-4-b223123e/",
                                                                                    "hint": "将MP发送给飞牛的路径进行替换。格式：MP路径|飞牛真实路径，每行一条",
                                                                                    "persistent-hint": True,
                                                                                    "rows": 2
                                                                                }
                                                                            }
                                                                        ]
                                                                    }
                                                                ]
                                                            },
                                                            {
                                                                "component": "VRow",
                                                                "content": [
                                                                    {"component": "VCol", "props": {"cols": 12}, "content": [
                                                                        {"component": "VSwitch", "props": {"model": "precision_scan_notify", "label": "开启通知", "color": "primary"}}
                                                                    ]}
                                                                ]
                                                            },
                                                            {
                                                                "component": "VRow",
                                                                "content": [
                                                                    {"component": "VCol", "props": {"cols": 12}, "content": [
                                                                        {"component": "VSelect", "props": {"model": "precision_scan_msgtype", "label": "通知渠道", "items": MsgTypeOptions, "hint": "选择精确扫描结果的通知渠道", "persistent-hint": True}}
                                                                    ]}
                                                                ]
                                                            },
                                                            {
                                                                "component": "VRow",
                                                                "content": [
                                                                    {"component": "VCol", "props": {"cols": 12}, "content": [
                                                                        {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "class": "mt-2"}, "content": [
                                                                            {"component": "div", "props": {"class": "text-caption"}, "content": [
                                                                                {"component": "div", "text": "本功能开启后，将启用指定文件夹扫描，可大幅缩短入库时间。"}
                                                                            ]}
                                                                        ]}
                                                                    ]}
                                                                ]
                                                            }
                                                        ]}
                                                    ]}
                                                ]}
                                            ]
                                        }
                                    ]}
                                ]
                            },
                            {
                                "component": "VWindowItem", "props": {"value": "tab-maintenance"},
                                "content": [
                                    {"component": "VCardText", "content": [
                                        {
                                            "component": "VRow",
                                            "content": [
                                                {"component": "VCol", "props": {"cols": 12}, "content": [
                                                    {"component": "VCard", "props": {"variant": "outlined", "class": "mb-3"}, "content": [
                                                        {"component": "VCardTitle", "props": {"class": "text-subtitle-1"}, "content": [{"component": "span", "text": "测试功能"}]},
                                                        {"component": "VDivider"},
                                                        {"component": "VCardText", "content": [
                                                            {
                                                                "component": "VRow",
                                                                "content": [
                                                                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VSwitch", "props": {"model": "run_once", "label": "媒体库获取测试", "color": "primary"}}]},
                                                                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VSwitch", "props": {"model": "check_tasks_once", "label": "获取扫描队列测试", "color": "primary"}}]},
                                                                ]
                                                            },
                                                                                                                    ]}
                                                    ]}
                                                ]}
                                            ]
                                        },
                                        {
                                            "component": "VRow",
                                            "content": [
                                                {"component": "VCol", "props": {"cols": 12}, "content": [
                                                    {"component": "VCard", "props": {"variant": "outlined", "class": "mb-3"}, "content": [
                                                        {"component": "VCardTitle", "props": {"class": "text-subtitle-1"}, "content": [{"component": "span", "text": "日志管理"}]},
                                                        {"component": "VDivider"},
                                                        {"component": "VCardText", "content": [
                                                            {
                                                                "component": "VRow",
                                                                "content": [
                                                                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VSwitch", "props": {"model": "auto_clear_log", "label": "自动清除日志", "color": "primary"}}]},
                                                                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VSwitch", "props": {"model": "clear_log_once", "label": "清除日志（单次）", "color": "primary"}}]},
                                                                ]
                                                            },
                                                            {
                                                                "component": "VRow",
                                                                "content": [
                                                                    {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VTextField", "props": {"model": "cron_schedule", "label": "自动清除日志Cron表达式", "placeholder": "0 8 * * 1", "hint": "五位Cron表达式，默认每周一早上8点执行", "persistent-hint": True}}]},
                                                                ]
                                                            }
                                                        ]}
                                                    ]}
                                                ]}
                                            ]
                                        }
                                    ]}
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
        
        default_values = {
            "enabled": self._enabled,
            "run_once": False,
            "scan_rules": self._scan_rules,
            "check_tasks_once": False,
            "selected_mediaservers": self._selected_mediaservers,
            "tab": "tab-basic", # Set initial tab to '基础设置'
            "clear_log_once": False, # 新增清除日志默认值
            "auto_clear_log": False, # 新增自动清除日志默认值
            "cron_schedule": self._cron_schedule, # 新增Cron表达式默认值
            "precision_scan_enabled": False, # 新增精确扫描默认值
            "precision_scan_notify": False, # 精确扫描通知开关默认值
            "precision_scan_msgtype": self._precision_scan_msgtype or "SiteMessage", # 通知类型默认值
            "path_mapping": self._path_mapping
        }
        
        return form_config, default_values

    
    
    def stop_service(self):
        """退出插件时停止所有调度器"""
        if self._task_scheduler and self._task_scheduler.running:
            # 注意：日志清理定时任务现在由MoviePilot框架管理，不需要手动移除
            self._task_scheduler.shutdown(wait=False)
            logger.info("【飞牛影视调度器】内部任务调度器已停止。")
        
        try:
            eventmanager.remove_event_listener(EventType.TransferComplete, self.handle_transfer_complete)
        except Exception as e:
            logger.debug(f"【飞牛影视调度器】注销事件监听器时出错（可能已被注销）: {e}")
        
        logger.info("【飞牛影视调度器】插件已停用。")
        return True

    # === 嵌套辅助类 ===

    @dataclass
    class PendingScanPath:
        """待扫描路径数据结构"""
        folder_path: str
        library_id: str
        library_name: str
        first_request_time: datetime
        last_request_time: datetime

        def __hash__(self):
            """基于路径和媒体库ID生成哈希，用于去重"""
            return hash((self.folder_path, self.library_id))

    class LibraryScanRequest:
        """媒体库扫描请求管理器"""
        def __init__(self, library_id: str, library_name: str):
            self.library_id = library_id
            self.library_name = library_name
            self.pending_paths = {}  # {normalized_path: Fnmvscheduler.PendingScanPath}
            self.debounce_job_id = f"debounce_precision_scan_{library_id}"

        def add_path(self, folder_path: str) -> bool:
            """添加路径，返回是否为新路径"""
            normalized_path = os.path.normpath(folder_path)
            current_time = datetime.now()

            if normalized_path in self.pending_paths:
                # 更新现有路径的最后请求时间
                self.pending_paths[normalized_path].last_request_time = current_time
                return False
            else:
                # 添加新路径
                path_request = Fnmvscheduler.PendingScanPath(
                    folder_path=normalized_path,
                    library_id=self.library_id,
                    library_name=self.library_name,
                    first_request_time=current_time,
                    last_request_time=current_time
                )
                self.pending_paths[normalized_path] = path_request
                return True

        def get_all_paths(self) -> List['Fnmvscheduler.PendingScanPath']:
            """获取所有待扫描路径"""
            return list(self.pending_paths.values())

        def get_path_count(self) -> int:
            """获取待扫描路径数量"""
            return len(self.pending_paths)

    @dataclass
    class TokenInfo:
        """Token信息数据结构"""
        token: str
        login_time: datetime
        api_instance: fnapi.Api
        base_url: str
        server_key: str

        def is_expired(self, max_age_minutes: int = 30) -> bool:
            """检查token是否过期"""
            return (datetime.now() - self.login_time).total_seconds() > max_age_minutes * 60

    class TokenManager:
        """Token管理器，用于缓存和复用登录token"""

        def __init__(self):
            self._token_cache: Dict[str, 'Fnmvscheduler.TokenInfo'] = {}  # {server_key: Fnmvscheduler.TokenInfo}
            self._lock = threading.Lock()
            self._api_key = "16CCEB3D-AB42-077D-36A1-F355324E4237"  # 飞牛API密钥

        def _generate_server_key(self, host: str, username: str) -> str:
            """生成服务器唯一标识"""
            return f"{host}#{username}"

        def _create_feiniu_api(self, host: str, api_key: str) -> Tuple[Optional[fnapi.Api], Optional[str]]:
            """创建飞牛API连接"""
            standard_host = UrlUtils.standardize_base_url(host).rstrip("/")
            host_with_v = f"{standard_host}/v"
            api = fnapi.Api(host_with_v, api_key)
            try:
                if api.sys_version(): return api, host_with_v
            except Exception: pass
            api = fnapi.Api(standard_host, api_key)
            try:
                if api.sys_version(): return api, standard_host
            except Exception: pass
            return None, None

        def _login_and_cache(self, host: str, username: str, password: str, api_key: str) -> Optional['Fnmvscheduler.TokenInfo']:
            """登录并缓存token"""
            try:
                logger.debug(f"【TokenManager】正在为服务器 {host} 用户 {username} 进行登录...")

                # 创建API连接
                api, base_url = self._create_feiniu_api(host, api_key)
                if not api:
                    logger.error(f"【TokenManager】无法创建API连接: {host}")
                    return None

                # 登录获取token
                token = api.login(username, password)
                if not token:
                    logger.error(f"【TokenManager】登录失败: {host}/{username}")
                    api.close()
                    return None

                # 创建token信息
                server_key = self._generate_server_key(host, username)
                token_info = Fnmvscheduler.TokenInfo(
                    token=token,
                    login_time=datetime.now(),
                    api_instance=api,
                    base_url=base_url,
                    server_key=server_key
                )

                logger.debug(f"【TokenManager】登录成功，缓存token: {server_key}")
                return token_info

            except Exception as e:
                logger.error(f"【TokenManager】登录过程中发生错误: {e}")
                return None

        def get_token(self, host: str, username: str, password: str, api_key: str = None) -> Tuple[Optional[str], Optional[fnapi.Api], Optional[str]]:
            """
            统一获取token的方法 - 懒加载模式

            :param host: 服务器地址
            :param username: 用户名
            :param password: 密码
            :param api_key: API密钥，可选
            :return: (token, api_instance, base_url)
            """
            if api_key is None:
                api_key = self._api_key

            server_key = self._generate_server_key(host, username)

            with self._lock:
                # 检查缓存中是否有该服务器的token
                if server_key in self._token_cache:
                    token_info = self._token_cache[server_key]

                    # 检查是否过期
                    if not token_info.is_expired():
                        logger.debug(f"【TokenManager】使用缓存的token: {server_key} (剩余有效时间: {30 - int((datetime.now() - token_info.login_time).total_seconds() / 60)}分钟)")
                        return token_info.token, token_info.api_instance, token_info.base_url
                    else:
                        logger.debug(f"【TokenManager】缓存的token已过期: {server_key}")
                        # 清理过期的token
                        del self._token_cache[server_key]

                # token不存在或已过期，重新登录
                logger.debug(f"【TokenManager】需要重新登录获取token: {server_key}")
                token_info = self._login_and_cache(host, username, password, api_key)

                if token_info:
                    # 缓存新的token
                    self._token_cache[server_key] = token_info
                    return token_info.token, token_info.api_instance, token_info.base_url
                else:
                    logger.error(f"【TokenManager】无法获取有效的token: {server_key}")
                    return None, None, None

        def invalidate_token(self, host: str, username: str):
            """手动使指定服务器的token失效"""
            server_key = self._generate_server_key(host, username)
            with self._lock:
                if server_key in self._token_cache:
                    del self._token_cache[server_key]
                    logger.debug(f"【TokenManager】已手动失效token: {server_key}")

        def clear_expired_tokens(self):
            """清理所有过期的token"""
            with self._lock:
                expired_keys = []
                for key, token_info in self._token_cache.items():
                    if token_info.is_expired():
                        expired_keys.append(key)

                for key in expired_keys:
                    del self._token_cache[key]
                    logger.debug(f"【TokenManager】清理过期token: {key}")

                if expired_keys:
                    logger.info(f"【TokenManager】清理了 {len(expired_keys)} 个过期token")

        def get_cache_info(self) -> Dict[str, Any]:
            """获取缓存信息（用于调试）"""
            with self._lock:
                return {
                    "total_cached": len(self._token_cache),
                    "servers": [
                        {
                            "server_key": key,
                            "login_time": token_info.login_time.isoformat(),
                            "age_minutes": int((datetime.now() - token_info.login_time).total_seconds() / 60),
                            "is_expired": token_info.is_expired()
                        }
                        for key, token_info in self._token_cache.items()
                    ]
                }

    def _send_precision_scan_notification(self, successful_paths: List[str], failed_paths: List[str]):
        """发送精确扫描结果通知"""
        # 只在有失败路径时才发送通知
        if not self._precision_scan_notify or not failed_paths:
            return

        try:
            title = "【飞牛影视调度器】"

            # 构建简化的通知内容
            text_parts = [
                "以下路径请求扫描失败："
            ]

            # 按序号列出失败的路径
            for i, path in enumerate(failed_paths, 1):
                text_parts.append(f"{i}. {path}")

            text = "\n".join(text_parts)

            # 设置通知类型
            mtype = NotificationType.SiteMessage
            if self._precision_scan_msgtype:
                try:
                    mtype = NotificationType[str(self._precision_scan_msgtype)]
                except Exception as e:
                    logger.error(f"通知类型 '{self._precision_scan_msgtype}' 无效，使用默认通知类型: {e}")

            # 发送通知
            self.post_message(mtype=mtype, title=title, text=text)
            logger.info("精确扫描失败路径通知已发送")

        except Exception as e:
            logger.error(f"发送精确扫描通知失败: {str(e)}")


class SignatureManager:
    """签名管理器，用于生成飞牛API的authx签名"""

    def __init__(self):
        # 飞牛API签名密钥
        self._secret_key = "NDzZTVxnRKP8Z0jXg1VAMonaG8akvh"

    def generate_authx_header(self, api_path: str, body: Optional[str], api_key: str) -> dict:
        """
        生成完整的authx签名信息
        按照飞牛官方API的算法生成

        :param api_path: API路径，如 "/api/v1/mdb/scan/library_id"
        :param body: 请求体内容
        :param api_key: API密钥
        :return: 包含nonce, timestamp, sign和完整authx头的字典
        """
        try:
            import time
            import random
            import hashlib
            import json

            logger.debug(f"【飞牛影视调度器】开始生成authx签名")
            logger.debug(f"  - API路径: {api_path}")
            logger.debug(f"  - 请求体: {body}")
            logger.debug(f"  - API密钥: {api_key}")

            # 确保api_path以/v开头
            if not api_path.startswith("/v"):
                api_path = "/v" + api_path

            # 生成随机nonce和时间戳
            nonce = str(random.randint(100000, 999999))
            timestamp = str(int(time.time() * 1000))

            logger.debug(f"  - 生成的nonce: {nonce}")
            logger.debug(f"  - 生成的时间戳: {timestamp}")

            # 计算请求体哈希
            md5 = hashlib.md5()
            md5.update((body or "").encode('utf-8'))
            data_hash = md5.hexdigest()

            logger.debug(f"  - 请求体哈希: {data_hash}")

            # 计算签名
            md5 = hashlib.md5()
            sign_string = "_".join([
                self._secret_key,
                api_path,
                nonce,
                timestamp,
                data_hash,
                api_key
            ])

            logger.debug(f"  - 签名字符串: {sign_string}")

            md5.update(sign_string.encode('utf-8'))
            sign = md5.hexdigest()

            logger.debug(f"  - 计算的签名: {sign}")

            # 构建完整的authx头
            authx_header = f"nonce={nonce}&timestamp={timestamp}&sign={sign}"

            logger.debug(f"【飞牛影视调度器】成功生成authx签名: {authx_header}")

            return {
                "nonce": nonce,
                "timestamp": timestamp,
                "sign": sign,
                "authx_header": authx_header,
                "api_path": api_path,
                "data_hash": data_hash,
                "sign_string": sign_string
            }

        except Exception as e:
            logger.error(f"【飞牛影视调度器】生成authx签名失败: {e}")
            # 返回默认的备用签名
            return {
                "nonce": "732840",
                "timestamp": "1759369686238",
                "sign": "",
                "authx_header": f"nonce=732840&timestamp=1759369686238&sign=",
                "api_path": api_path,
                "data_hash": "",
                "sign_string": ""
            }

