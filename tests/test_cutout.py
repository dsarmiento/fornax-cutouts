"""Temp file for testing cutouts"""

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from fornax_cutouts.jobs.tasks import generate_color_preview, generate_cutout
from fornax_cutouts.models.base import TargetPosition

CUTOUT_FILE_ASDF = "tests/data/r00342_p_v01002001004008_008m46x76y60_f146_coadd_shrink.asdf"
CUTOUT_FILE_FITS = (
    "s3://stpubdata/panstarrs/ps1/public/md10.v3.skycell/md10/083/deep/md10.v3.skycell.083.stk_deep.y.unconv.fits"
)


def make_fitscutout_mock(*_args, **_kwargs):
    mock_fits_cutout = mock.MagicMock()
    mock_fits_cutout.write_as_fits = mock.MagicMock(return_value=["mocked_fits_file.fits"])
    return mock_fits_cutout


@mock.patch("astrocut.FITSCutout", make_fitscutout_mock)
@mock.patch("fornax_cutouts.jobs.tasks.setup_filesystem", mock.MagicMock())
@mock.patch("os.stat", mock.MagicMock(return_value=mock.MagicMock(st_size=12345)))
@mock.patch("fornax_cutouts.jobs.tasks.FITSCutoutHandler.get_filter", mock.MagicMock(return_value="abc"))
def test_generate_cutout_fits():
    """Test that we can generate a cutout from a FITS file"""
    response = generate_cutout(
        source_file=CUTOUT_FILE_FITS,
        target=TargetPosition(ra=0.0, dec=0.0),
        size=(100, 100),
        output_dir="testdir",
    )
    assert response.science == "mocked_fits_file.fits"
    assert response.size_px == (100, 100)
    assert response.filter == "abc"


def test_generate_cutout_asdf():
    """Test that we can generate a cutout from an ASDF file"""
    cutout_ra = 8.46340835
    cutout_dec = -43.8514
    with TemporaryDirectory() as tmpdir:
        response = generate_cutout(
            source_file=CUTOUT_FILE_ASDF,
            target=TargetPosition(ra=cutout_ra, dec=cutout_dec),
            size=(10, 10),
            output_dir=tmpdir,
        )
        cutout_stem = Path(CUTOUT_FILE_ASDF).stem
        expected_filename = f"{tmpdir}/{cutout_stem}_{cutout_ra:.7f}_{cutout_dec:.7f}_10-x-10_astrocut.asdf"
        assert response.science == expected_filename
        assert response.preview is None
        assert response.size_px == (10, 10)
        assert response.filter == "F146"


def test_generate_preview_asdf():
    """Test that we can generate a cutout preview from an ASDF file"""
    cutout_ra = 8.46340835
    cutout_dec = -43.8514
    with TemporaryDirectory() as tmpdir:
        response = generate_cutout(
            source_file=CUTOUT_FILE_ASDF,
            target=TargetPosition(ra=cutout_ra, dec=cutout_dec),
            size=(10, 10),
            output_dir=tmpdir,
            generate_preview=True,
            generate_science=False,
        )
        cutout_stem = Path(CUTOUT_FILE_ASDF).stem
        expected_filename = f"{tmpdir}/{cutout_stem}_{cutout_ra:.7f}_{cutout_dec:.7f}_10-x-10_astrocut_0.jpg"
        assert response.science is None
        assert response.preview == expected_filename
        assert response.size_px == (10, 10)
        assert response.filter == "F146"


def test_generate_color_preview():
    """Test that we can generate a color preview from a FITS file"""
    cutout_ra = 188.27856215089
    cutout_dec = 82.56394517878
    cutout_size = (10, 10)
    target = TargetPosition(ra=float(cutout_ra), dec=float(cutout_dec))
    cutout_files = [
        "tests/data/rings.v3.skycell.2627.066.stk.i.unconv_shrink.fits",
        "tests/data/rings.v3.skycell.2627.066.stk.g.unconv_shrink.fits.gz",
        "tests/data/rings.v3.skycell.2627.066.stk.r.unconv_shrink.fits",
    ]

    with TemporaryDirectory() as tmpdir:
        response = generate_color_preview(
            cutout_files[0],
            cutout_files[1],
            cutout_files[2],
            target,
            cutout_size,
            output_dir=tmpdir,
        )

        cutout_stem = Path(cutout_files[0]).stem
        expected_filename = f"{tmpdir}/{cutout_stem}_color_{cutout_ra:.7f}_{cutout_dec:.7f}_10-x-10_astrocut.jpg"

        assert response.preview == expected_filename
        assert response.science is None
        assert response.size_px == (10, 10)
