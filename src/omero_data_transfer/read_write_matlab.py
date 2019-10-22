import os
from scipy import io as sio

PROJECT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..")

def load_matlab_object():
    dir_path = os.path.join(PROJECT_DIR, "tests", "test_data", "matlab")
    # dbTable4287 = sio.loadmat(os.path.join(dir_path, 'dbTable4287.mat'))
    dbTable4287 = sio.loadmat(os.path.join(dir_path, 'dbInfo4287.mat'))
    print dbTable4287

    dbData = sio.loadmat(os.path.join(dir_path, 'dbData.mat'))

    print dbData


def main():
    load_matlab_object()


if __name__ == "__main__":
    main()