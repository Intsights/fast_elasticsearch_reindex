import setuptools


setuptools.setup(
    name='fast_elasticsearch_reindex',
    version='1.0.0',
    packages=setuptools.find_packages(),
    install_requires=[
        'elasticsearch',
        'orjson',
        'tenacity',
        'tqdm',
    ],
)
