import polars as pl
from fhir2meds.meds_writer import robust_cast_time_column
import datetime

def test_robust_cast_time_column():
    # Test cases: Z, +HH:MM, -HH:MM, only hours offset, milliseconds, no seconds, and no offset
    times = [
        "2148-07-07T16:18:13Z",
        "2148-07-07T16:18:13-04:00",
        "2148-07-07T16:18:13+02:00",
        "2148-07-07T16:18:13",
        "2148-07-07T16:18:13.123Z",
        "2148-07-07T16:18:13.123-04:00",
        "2148-07-07T16:18:13.123+02:00",
        "2148-07-07T16:18:13.123",
        "2148-07-07T16:18Z",
        "2148-07-07T16:18-04:00",
        "2148-07-07T16:18+02:00",
        "2148-07-07T16:18",
        None,
    ]
    pl_df = pl.DataFrame({"time": times})
    pl_df = robust_cast_time_column(pl_df)
    result = pl_df["time"].to_list()
    print("Parsed results:", result)
    expected = [
        datetime.datetime(2148, 7, 7, 16, 18, 13),
        datetime.datetime(2148, 7, 7, 16, 18, 13),
        datetime.datetime(2148, 7, 7, 16, 18, 13),
        datetime.datetime(2148, 7, 7, 16, 18, 13),
        datetime.datetime(2148, 7, 7, 16, 18, 13, 123000),
        datetime.datetime(2148, 7, 7, 16, 18, 13, 123000),
        datetime.datetime(2148, 7, 7, 16, 18, 13, 123000),
        datetime.datetime(2148, 7, 7, 16, 18, 13, 123000),
        datetime.datetime(2148, 7, 7, 16, 18),
        datetime.datetime(2148, 7, 7, 16, 18),
        datetime.datetime(2148, 7, 7, 16, 18),
        datetime.datetime(2148, 7, 7, 16, 18),
        None,
    ]
    for idx, (input_val, r, e) in enumerate(zip(times, result, expected)):
        if e is None:
            assert r is None, f"Case {idx}: Input '{input_val}' - Expected None, got {r}"
        else:
            assert r is not None, f"Case {idx}: Input '{input_val}' - Expected {e}, got None"
            assert r.replace(tzinfo=None) == e, f"Case {idx}: Input '{input_val}' - Expected {e}, got {r}" 