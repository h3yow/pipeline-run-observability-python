"""骨架冒烟测试。"""


def test_skeleton_imports():
    from app.main import app  # noqa: F401

    assert app is not None
