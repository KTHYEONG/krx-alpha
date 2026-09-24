from src.core.symbols import is_krx_short_code


def test_is_krx_short_code_accepts_numeric_and_alphanumeric_codes() -> None:
    for code in ("005930", "0007C0", "0161M0"):
        assert is_krx_short_code(code) is True


def test_is_krx_short_code_rejects_malformed_codes() -> None:
    for code in ("00593", "0059300", "0007c0", " 05930", "005930 ", "", None, 5930):
        assert is_krx_short_code(code) is False
