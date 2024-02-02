if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    zero_passenger_count = data['passenger_count'].isin([0]).sum()
    print(f"Rows with zero passengers: {zero_passenger_count}")

    return data[data['passenger_count'] > 0]



@test
def test_output(output, *args):
    assert output['passenger_count'].isin([0]).sum() == 0, "There are rides with zero passengers"
    