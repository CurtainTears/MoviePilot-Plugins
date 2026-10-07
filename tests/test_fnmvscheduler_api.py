"""飞牛 API 兼容性回归：旧宿主无私有会话，新宿主负责请求签名。"""
import hashlib
import json
from types import SimpleNamespace
from urllib.parse import parse_qs
from unittest.mock import Mock

import pytest
import requests

from fnmvscheduler import Fnmvscheduler, fnapi


class LegacyApi:
    """模拟只提供公开请求接口、不含 _session 的宿主API。"""

    __slots__ = ("request",)

    def __init__(self, response):
        self.request = Mock(return_value=response)


@pytest.fixture
def plugin():
    """创建无需调度器或外部账号的插件实例。"""
    return object.__new__(Fnmvscheduler)


def result(data=None, success=True):
    """构造宿主公开接口的业务结果。"""
    return SimpleNamespace(success=success, data=data)


def test_running_tasks_without_private_session(plugin):
    """旧宿主也能读取任务GUID并去重。"""
    api = LegacyApi(result([{'guid': 'library-a'}, {'guid': 'library-a'},
                            {'guid': 'library-b'}, {}, {'guid': ''}]))
    assert not hasattr(api, '_session')
    assert set(plugin._get_running_tasks(api, 'http://nas.invalid/v', 'fixture-token')) == {
        'library-a', 'library-b',
    }
    api.request.assert_called_once_with('/task/running')


@pytest.mark.parametrize('data', [None, []])
def test_running_tasks_empty_response(plugin, data):
    """没有任务时返回空列表。"""
    api = LegacyApi(result(data))
    assert plugin._get_running_tasks(api, 'http://nas.invalid', 'fixture-token') == []


@pytest.mark.parametrize('response', [None, result(success=False)])
def test_running_tasks_failed_response(plugin, response):
    """保持请求失败时原有的返回约定。"""
    api = LegacyApi(response)
    assert plugin._get_running_tasks(api, 'http://nas.invalid', 'fixture-token') == []


def test_media_libraries_without_private_session(plugin):
    """媒体库查询也不依赖宿主内部会话。"""
    libraries = [{'guid': 'library-a', 'name': '电影'}]
    api = LegacyApi(result(libraries))
    assert plugin._get_media_libraries(api, 'http://nas.invalid/v', 'fixture-token') == libraries
    api.request.assert_called_once_with('/mdb')


@pytest.mark.parametrize('response, expected', [
    (result(), True), (result(success=False), False), (None, False),
])
def test_folder_scan_without_private_session(plugin, response, expected):
    """文件夹扫描使用宿主公开POST接口并保留成功判断。"""
    api = LegacyApi(response)
    paths = ['/media/电影']
    assert plugin._scan_folder(api, 'http://nas.invalid/v', 'fixture-token',
                               'library-a', paths) is expected
    api.request.assert_called_once_with('/mdb/scan/library-a', method='post',
                                        data={'dir_list': paths})


@pytest.mark.parametrize('response, expected', [
    (result(), True), (result(success=False), False), (None, False),
])
def test_precision_scan_without_private_session(plugin, response, expected):
    """精确扫描保留中文路径映射并委托宿主生成签名。"""
    plugin._path_mapping = '/media|/飞牛媒体'
    api = LegacyApi(response)
    lib = SimpleNamespace(id='library-a', name='电影')
    assert plugin._scan_single_path_with_precision(
        api, 'http://nas.invalid/v', 'fixture-token', lib, None, '/media/中文 电影',
    ) is expected
    api.request.assert_called_once_with('/mdb/scan/library-a', method='post',
                                        data={'dir_list': ['/飞牛媒体/中文 电影']})


@pytest.mark.parametrize('operation', ['tasks', 'libraries', 'scan', 'precision'])
@pytest.mark.parametrize('host', ['http://nas.invalid', 'http://nas.invalid/v'])
def test_real_host_request_authentication_and_signature(plugin, monkeypatch, operation, host):
    """真实宿主发送的认证和签名与实际URL、中文请求体一致。"""
    api = fnapi.Api(host, 'fixture-api-key')
    api._token = 'fixture-token'
    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps({'code': 0, 'data': [{'guid': 'library-a'}]}).encode()
    transport = Mock(return_value=response)
    monkeypatch.setattr(api._request_utils, 'request', transport)
    lib = SimpleNamespace(id='library-a', name='电影')
    try:
        if operation == 'tasks':
            assert plugin._get_running_tasks(api, host, 'fixture-token') == ['library-a']
            path = '/task/running'
        elif operation == 'libraries':
            assert plugin._get_media_libraries(api, host, 'fixture-token') == [{'guid': 'library-a'}]
            path = '/mdb'
        elif operation == 'scan':
            assert plugin._scan_folder(api, host, 'fixture-token', lib.id, ['/media/中文 电影'])
            path = '/mdb/scan/library-a'
        else:
            assert plugin._scan_single_path_with_precision(
                api, host, 'fixture-token', lib, None, '/media/中文 电影',
            )
            path = '/mdb/scan/library-a'
        transport.assert_called_once()
        request = transport.call_args.kwargs
        assert request['url'] == host + '/api/v1' + path
        assert request['headers']['Authorization'] == 'fixture-token'
        body = request['data'] or ''
        if operation in ['scan', 'precision']:
            assert request['method'] == 'post'
            assert json.loads(body) == {'dir_list': ['/media/中文 电影']}
        else:
            assert request['method'] == 'get'
        authx = parse_qs(request['headers']['authx'])
        body_hash = hashlib.md5(body.encode()).hexdigest()
        signed = '_'.join(['NDzZTVxnRKP8Z0jXg1VAMonaG8akvh', '/v/api/v1' + path,
                           authx['nonce'][0], authx['timestamp'][0], body_hash,
                           'fixture-api-key'])
        assert authx['sign'][0] == hashlib.md5(signed.encode()).hexdigest()
    finally:
        api.close()
