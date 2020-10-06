import setuptools


setuptools.setup(
    name='fast_elasticsearch_reindex',
    description="A Python based alternative to Elasticsearch Reindex API with multiprocessing support",
    version='1.0.0',
    author="Adir Haleli",
    author_email="adir544@gmail.com",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    install_requires=[
        'elasticsearch',
        'orjson',
        'tenacity',
        'tqdm',
    ],
    url="https://github.com/Intsights/fast_elasticsearch_reindex",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.4',
)
