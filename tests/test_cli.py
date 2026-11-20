from __future__ import annotations

from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlsplit

import pytest

from conftest import PALU_EVENTS, fixture
from quake_lake.cli import main
from quake_lake.storage import LocalStorage


def test_backfill_writes_one_object_per_month(net, tmp_path, capsys):
    bronze = tmp_path / "bronze"
    assert (
        main(["usgs", "--since", "2018-08-15", "--until", "2018-10-01", "--bronze", str(bronze)])
        == 0
    )

    keys = LocalStorage(bronze).list()
    assert [k.split("/")[-1] for k in keys] == [
        "20180815T000000Z_20180901T000000Z.json",
        "20180901T000000Z_20181001T000000Z.json",
    ]
    out = capsys.readouterr().out.splitlines()
    assert out[0] == f"{keys[0]}  {PALU_EVENTS} events  57,216 bytes"
    assert out[-1].startswith(f"wrote 2 objects to {bronze}: {2 * PALU_EVENTS} events")
    assert len(net.urls) == 2


def test_default_is_the_last_seven_days(net, tmp_path, capsys):
    assert main(["usgs", "--bronze", str(tmp_path)]) == 0
    (url,) = net.urls
    q = {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}
    assert datetime.fromisoformat(q["endtime"]) - datetime.fromisoformat(
        q["starttime"]
    ) == timedelta(days=7)
    assert "wrote 1 object to " in capsys.readouterr().out


def test_a_future_until_is_clamped_to_now(net, tmp_path):
    since = (datetime.now(UTC) - timedelta(days=3)).date().isoformat()
    assert main(["usgs", "--since", since, "--until", "2999-01-01", "--bronze", str(tmp_path)]) == 0
    end = LocalStorage(tmp_path).list()[-1].rsplit("_", 1)[1].removesuffix(".json")
    assert abs(
        datetime.strptime(end, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC) - datetime.now(UTC)
    ) < timedelta(minutes=1)


def test_bmkg(net, tmp_path, capsys):
    assert main(["bmkg", "--bronze", str(tmp_path)]) == 0
    assert len(LocalStorage(tmp_path).list()) == 3
    assert "31 events" in capsys.readouterr().out.splitlines()[-1]


def test_a_broken_bmkg_feed_does_not_stop_the_others(net, tmp_path, capsys):
    net.routes["https://data.bmkg.go.id/DataMKG/TEWS/gempaterkini.json"] = 500
    assert main(["bmkg", "--bronze", str(tmp_path)]) == 1
    names = [k.rsplit("/", 1)[1].split("_")[0] for k in LocalStorage(tmp_path).list()]
    assert names == ["autogempa", "gempadirasakan"]
    out, err = capsys.readouterr()
    assert "gempaterkini.json failed after 4 attempts: HTTP 500" in err
    assert out.splitlines()[-1].startswith("wrote 2 objects to ")


def test_bronze_defaults_to_the_environment(net, tmp_path, monkeypatch):
    monkeypatch.setenv("BRONZE_URI", str(tmp_path / "from-env"))
    assert main(["bmkg"]) == 0
    assert len(LocalStorage(tmp_path / "from-env").list()) == 3


def test_a_failure_part_way_reports_what_landed(net, tmp_path, capsys):
    net.routes["https://earthquake.usgs.gov/"] = [fixture("usgs-empty.json"), 400]
    code = main(
        ["usgs", "--since", "2018-07-01", "--until", "2018-10-01", "--bronze", str(tmp_path)]
    )
    out, err = capsys.readouterr()
    assert code == 1
    assert len(net.urls) == 2  # stopped at the failure: the third month was never asked for
    assert len(LocalStorage(tmp_path).list()) == 1
    assert "20180701T000000Z_20180801T000000Z.json" in out
    assert err.startswith("error: GET https://earthquake.usgs.gov/")
    assert err.endswith("failed: HTTP 400: server says no\n")


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (
            ["usgs", "--since", "2020-01-01", "--until", "2019-01-01"],
            "--since (2020-01-01T00:00:00+00:00) must be before --until (2019-01-01T00:00:00+00:00)",
        ),
        (["usgs", "--bronze", "gs://bucket/x"], "unsupported storage URI"),
        (["usgs", "--bronze", ""], "empty storage URI"),
    ],
)
def test_errors_exit_1_with_a_message(net, argv, message, capsys):
    assert main(argv) == 1
    assert message in capsys.readouterr().err
    assert net.urls == []


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (["usgs", "--until", "2020-01-01"], "--until needs --since"),
        (["usgs", "--days", "0"], "whole number of days"),
        (["usgs", "--days", "-3"], "whole number of days"),
        (["usgs", "--days", "999999999"], "from 1 to 36500"),
        (["usgs", "--since", "yesterday"], "expected an ISO date"),
        (["usgs", "--days", "3", "--since", "2020-01-01"], "not allowed with"),
        ([], "required"),
    ],
)
def test_usage_errors_exit_2(argv, message, capsys):
    with pytest.raises(SystemExit) as e:
        main(argv)
    assert e.value.code == 2
    assert message in capsys.readouterr().err


def test_timezone_in_since_is_converted_to_utc(net, tmp_path):
    assert (
        main(
            [
                "usgs",
                "--since",
                "2018-09-01T07:00:00+07:00",
                "--until",
                "2018-09-02",
                "--bronze",
                str(tmp_path),
            ]
        )
        == 0
    )
    assert LocalStorage(tmp_path).list()[0].endswith("/20180901T000000Z_20180902T000000Z.json")
