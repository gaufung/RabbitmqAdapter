# -*- coding: utf-8 -*-
"""
A http related util tools
"""
from __future__ import unicode_literals
from __future__ import print_function
import urllib
import urlparse
import mimetypes
from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from tornado.httpclient import HTTPRequest


def generate_url(url, router_path, protocol="http", query_args=None):
    """
    encode url
    :param url: basic url
    :param router_path: router path
    :param protocol: protocol: http or ws (web socket)
    :param query_args: query arguments
    :return: encoded url
    """
    if protocol not in ("http", "ws"):
        raise Exception("unsupported protocol: %s" % protocol)
    split_result = urlparse.urlsplit(url)
    scheme = split_result[0]
    path = split_result[1]+split_result[2]
    if scheme in ("https", "wss"):
        protocol += "s"
    new_url = protocol + "://" + path.rstrip("/") + router_path
    if query_args:
        new_url += "?" + urllib.urlencode(query_args)
    return new_url


@gen.coroutine
def post_multi_files(url, fields, files, headers=None):
    content_type, body = _encode_multi_file(fields, files)
    if headers is None:
        headers = {}
    headers['Content-Type'] = content_type
    headers['content-length'] = str(len(body))
    request = HTTPRequest(url, "POST", headers=headers, body=body, validate_cert=False)
    response = yield AsyncHTTPClient().fetch(request, raise_error=False)
    raise gen.Return(response)


def _encode_multi_file(fields, files):
    boundary = '----------ThIs_Is_tHe_bouNdaRY_$'
    l = []
    for (key, value) in fields:
        l.append('--' + boundary)
        l.append('Content-Disposition: form-data; name="%s"' % key)
        l.append('')
        l.append(value)
    for (key, filename, value) in files:
        if isinstance(filename, unicode):
            filename = filename.encode("utf8")
        l.append('--' + boundary)
        l.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (key, filename))
        l.append('Content-Type: %s' % _get_content_type(filename))
        l.append('')
        l.append(bytearray(value))
    l.append('--' + boundary + '--')
    l.append('')
    body = bytearray('\r\n').join(l)
    body = str(body)
    content_type = 'multipart/form-data; boundary=%s' % boundary
    return content_type, body


def _get_content_type(file_name):
    return mimetypes.guess_type(file_name)[0] or 'application/octet-stream'
