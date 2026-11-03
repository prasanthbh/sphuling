import pytest
import datafusion as df
from pysphuling.sql import functions as F
from pysphuling.sql.column import Column
from pysphuling.sql.window import Window


def test_column_arithmetic():
    c1 = F.col("a")
    c2 = F.col("b")

    add_c = c1 + c2
    assert isinstance(add_c, Column)

    mul_c = c1 * 10
    assert isinstance(mul_c, Column)

    div_c = c1 / 2.5
    assert isinstance(div_c, Column)


def test_column_comparisons():
    c = F.col("salary")
    cond = (c > 50000) & (c <= 100000)
    assert isinstance(cond, Column)

    eq_null_safe = c.eqNullSafe(50000)
    assert isinstance(eq_null_safe, Column)


def test_when_otherwise():
    c = F.col("score")
    grade = (
        F.when(c >= 90, "A")
        .when(c >= 80, "B")
        .otherwise("C")
    )
    assert isinstance(grade, Column)


def test_window_specification():
    w = Window.partitionBy("dept").orderBy(F.col("salary").desc())
    rn = F.row_number().over(w)
    assert isinstance(rn, Column)

    lead_col = F.lead("salary", 1).over(w)
    assert isinstance(lead_col, Column)

