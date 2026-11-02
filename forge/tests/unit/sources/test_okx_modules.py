import pytest

from marketforge.acquisition.sources.okx import OKXSource
from marketforge.models import DataType


def test_trade_ticks_use_module_1():
    assert OKXSource._module(DataType.TRADE_TICKS) == 1


def test_order_book_l2_uses_module_5():
    assert OKXSource._module(DataType.ORDER_BOOK_L2) == 5
