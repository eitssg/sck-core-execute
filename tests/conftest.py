def pytest_addoption(parser):
    parser.addoption(
        "--real-aws",
        action="store_true",
        default=False,
        help="Run tests with real AWS integration",
    )
