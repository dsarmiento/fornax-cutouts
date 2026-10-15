from click.testing import CliRunner

from fornax_cutouts import __version__
from fornax_cutouts.cli import cli


def test_version_flag():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert result.output.strip() == f"fornax-cutouts, version {__version__}"


def test_version_short_flag():
    runner = CliRunner()
    result = runner.invoke(cli, ["-V"])
    assert result.exit_code == 0
    assert __version__ in result.output
