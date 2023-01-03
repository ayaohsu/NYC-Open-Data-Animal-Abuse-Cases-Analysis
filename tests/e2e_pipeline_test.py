import pytest

from main import run_etl_pipeline

@pytest.fixture(autouse=True)
def set_up_tear_down():
    
    print('Setting up')
    
    yield
    
    print('Tearing down')

def test_e2e():
    print('Testing')