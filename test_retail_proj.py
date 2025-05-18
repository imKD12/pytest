import pytest
from lib.Utils import get_spark_session
from lib.DataReader import read_customers,read_orders
from lib.DataManipulation import filter_closed_orders,filter_orders_generic

@pytest.mark.skip("work in progress")
def test_read_customer_df(spark):
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count == 12435

@pytest.mark.skip("work in progress")
def test_read_orders_df(spark):
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68884

@pytest.mark.transformation
def test_filtered_record(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_closed_orders(orders_df).count()
    assert filtered_count == 7556

@pytest.mark.parametrize(
    "status,count",
    [("CLOSED",7556),
     ("PENDING_PAYMENT",15030),
     ("COMPLETE",22900)
    ]
)
def test_check_parameter_marker(spark,status,count):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_orders_generic(orders_df,status).count()
    assert filtered_count == count
    