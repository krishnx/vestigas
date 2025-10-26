import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend.orm_models import Base

# Use an in-memory SQLite database for tests
TEST_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(scope="function")
def override_get_db():
    """
    Provides a fresh SQLAlchemy session for each test.
    Drops and recreates tables to ensure test isolation.
    """
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()
