if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd 
import re

@transformer
def transform(data, *args, **kwargs):
    df = data
    #Remove rows where the passenger count is equal to 0 or the trip distance is equal to zero.
    df = df[(df['passenger_count'] > 0) & (df['trip_distance'] > 0)]
    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date
    # Rename column to SnakeCase
    for col in df.columns:
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', col)
        new_col = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
        df.columns = df.columns.str.replace(col, new_col)
    #return data
    return df 

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output.vendor_id is not None, 'Required column doesnt exist'
    assert len(output[output['passenger_count'] == 0]) == 0, 'Passenger_count is greater than 0'
    assert len(output[output['trip_distance'] == 0]) == 0, 'Trip distance is greater than 0'
