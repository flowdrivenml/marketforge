from unittest.mock import Mock, patch

import pytest
import requests

from marketforge.acquisition.http import HttpClient, RequestPolicy
from marketforge.acquisition.sources.gateio import GATEIO_ARCHIVE_POLICY, GateIOSource
from marketforge.errors import AcquisitionError


def make_response(
    status_code: int,
) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.url = "https://example.com/test"
    return response


def test_success_first_attempt():
    client = HttpClient()

    client.session.request = Mock(return_value=make_response(200))

    response = client.get(
        "https://example.com/test",
        policy=RequestPolicy(
            request_interval=0,
            max_retries=3,
            backoff=0,
        ),
    )

    assert response.status_code == 200
    assert client.session.request.call_count == 1


@pytest.mark.parametrize(
    "status_code",
    [
        429,
        500,
        502,
        503,
        504,
    ],
)
def test_retryable_status_then_success(
    status_code,
):
    client = HttpClient()

    client.session.request = Mock(
        side_effect=[
            make_response(status_code),
            make_response(200),
        ]
    )

    response = client.get(
        "https://example.com/test",
        policy=RequestPolicy(
            request_interval=0,
            max_retries=3,
            backoff=0,
        ),
    )

    assert response.status_code == 200
    assert client.session.request.call_count == 2


@pytest.mark.parametrize(
    "status_code",
    [
        400,
        401,
        403,
        404,
    ],
)
def test_non_retryable_status(
    status_code,
):
    client = HttpClient()

    client.session.request = Mock(return_value=make_response(status_code))

    with pytest.raises(AcquisitionError):
        client.get(
            "https://example.com/test",
            policy=RequestPolicy(
                request_interval=0,
                max_retries=3,
                backoff=0,
            ),
        )

    assert client.session.request.call_count == 1


def test_timeout_then_success():
    client = HttpClient()

    client.session.request = Mock(
        side_effect=[
            requests.Timeout("temporary timeout"),
            make_response(200),
        ]
    )

    response = client.get(
        "https://example.com/test",
        policy=RequestPolicy(
            request_interval=0,
            max_retries=3,
            backoff=0,
        ),
    )

    assert response.status_code == 200
    assert client.session.request.call_count == 2


def test_connection_error_then_success():
    client = HttpClient()

    client.session.request = Mock(
        side_effect=[
            requests.ConnectionError("temporary connection failure"),
            make_response(200),
        ]
    )

    response = client.get(
        "https://example.com/test",
        policy=RequestPolicy(
            request_interval=0,
            max_retries=3,
            backoff=0,
        ),
    )

    assert response.status_code == 200
    assert client.session.request.call_count == 2


def test_retry_exhaustion():
    client = HttpClient()

    client.session.request = Mock(return_value=make_response(503))

    with pytest.raises(AcquisitionError):
        client.get(
            "https://example.com/test",
            policy=RequestPolicy(
                request_interval=0,
                max_retries=3,
                backoff=0,
            ),
        )

    # Initial request + 3 retries.
    assert client.session.request.call_count == 4


@patch("marketforge.acquisition.http.time.sleep")
def test_exponential_backoff(
    mock_sleep,
):
    client = HttpClient()

    client.session.request = Mock(
        side_effect=[
            make_response(503),
            make_response(503),
            make_response(503),
            make_response(200),
        ]
    )

    response = client.get(
        "https://example.com/test",
        policy=RequestPolicy(
            request_interval=0,
            max_retries=3,
            backoff=1.0,
        ),
    )

    assert response.status_code == 200

    assert mock_sleep.call_args_list == [
        ((1.0,),),
        ((2.0,),),
        ((4.0,),),
    ]


def test_allowed_status_code_is_returned():
    client = HttpClient()

    client.session.request = Mock(return_value=make_response(404))

    response = client.head(
        "https://example.com/missing.csv.gz",
        policy=RequestPolicy(
            request_interval=0,
            max_retries=3,
            backoff=0,
        ),
        allowed_status_codes={404},
    )

    assert response.status_code == 404
    assert client.session.request.call_count == 1


def test_404_without_allowed_status_still_raises():
    client = HttpClient()

    client.session.request = Mock(return_value=make_response(404))

    with pytest.raises(AcquisitionError):
        client.head(
            "https://example.com/missing.csv.gz",
            policy=RequestPolicy(
                request_interval=0,
                max_retries=3,
                backoff=0,
            ),
        )

    assert client.session.request.call_count == 1


from unittest.mock import Mock

import requests

from marketforge.acquisition.sources.gateio import GateIOSource


def make_head_response(
    status_code: int,
) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.url = "https://download.gatedata.org/test"
    return response


def test_archive_exists_for_200():
    source = GateIOSource()

    source.http.head = Mock(return_value=make_head_response(200))

    assert source._archive_exists("https://download.gatedata.org/test")


def test_archive_missing_for_404():
    source = GateIOSource()

    source.http.head = Mock(return_value=make_head_response(404))

    assert not source._archive_exists("https://download.gatedata.org/test")

    source.http.head.assert_called_once()
