from setuptools import setup

# Create a wheel package
# python setup.py && rm -vrf *.egg-info ./lib *.win-amd64

# Install a wheel package
# pip install kommati_para-0.0.1-py3-none-any.whl --force-reinstall

setup(
    name='kommati_para',
    version='0.0.1',
    description='A package for processing two csv files with PySpark',
    packages=['kommati_para'],
    package_dir={'kommati_para': '.'},
    script_args=[
        'bdist_wheel',
        f'--dist-dir=./wheels'
    ],
    options={'build': {'build_base': '.'}},
    install_requires=[
        'pyspark>=3.5.1'
    ],
    python_requires='>=3.10',
)
