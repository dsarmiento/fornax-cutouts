from __future__ import annotations

from unittest.mock import patch

from starlette.requests import Request

from fornax_cutouts.utils.middleware import client_ip_from_request

LOAD_BALANCER_IP = "10.146.26.7"
CLIENT_IP = "1.2.3.4"
SPOOFED_IP = "9.9.9.9"
UPSTREAM_PROXY_IP = "70.0.0.1"  # first trusted hop before ALB (N=2 scenario)
REAL_IP = "8.8.8.8"


def _request(
    headers: dict[str, str] | None = None,
    client: tuple[str, int] | None = (LOAD_BALANCER_IP, 12345),
) -> Request:
    encoded = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "headers": encoded,
        "client": client,
        "server": ("127.0.0.1", 80),
    }
    return Request(scope)


@patch("fornax_cutouts.utils.middleware.CONFIG.num_trusted_proxies", 0)
def test_zero_trusted_proxies_returns_peer():
    request = _request(headers={"x-forwarded-for": CLIENT_IP})
    assert client_ip_from_request(request) == LOAD_BALANCER_IP


@patch("fornax_cutouts.utils.middleware.CONFIG.num_trusted_proxies", 1)
def test_one_trusted_proxy_honest_client():
    request = _request(headers={"x-forwarded-for": CLIENT_IP})
    assert client_ip_from_request(request) == CLIENT_IP


@patch("fornax_cutouts.utils.middleware.CONFIG.num_trusted_proxies", 1)
def test_one_trusted_proxy_ignores_spoofed_xff():
    request = _request(headers={"x-forwarded-for": f"{SPOOFED_IP}, {CLIENT_IP}"})
    assert client_ip_from_request(request) == CLIENT_IP


@patch("fornax_cutouts.utils.middleware.CONFIG.num_trusted_proxies", 2)
def test_two_trusted_proxies():
    request = _request(headers={"x-forwarded-for": f"{CLIENT_IP}, {UPSTREAM_PROXY_IP}"})
    assert client_ip_from_request(request) == CLIENT_IP


@patch("fornax_cutouts.utils.middleware.CONFIG.num_trusted_proxies", 2)
def test_xff_shorter_than_trusted_proxies_falls_back_to_peer():
    request = _request(headers={"x-forwarded-for": CLIENT_IP})
    assert client_ip_from_request(request) == LOAD_BALANCER_IP


@patch("fornax_cutouts.utils.middleware.CONFIG.num_trusted_proxies", 1)
def test_one_trusted_proxy_real_ip_when_xff_absent():
    request = _request(headers={"x-real-ip": REAL_IP})
    assert client_ip_from_request(request) == REAL_IP
