import dicom
import os, sys, getopt, glob

#  Constants
HELP_MSG = 'GenSeqFile.py <path to images.'
SYNC_ESCAPE = -1
SYNC_HASH_SIZE = 16
SYNC_SIZE = 4 + SYNC_HASH_SIZE
SYNC_INTERVAL = 100 * SYNC_SIZE
SYNC_MARK = os.urandom(16)  # 128 bits unique number

SAMPLE_PATH = ''
OUTPUT_FILENAME = 'sample.dat'


def boolean2bytes(value):
    assert isinstance(value, bool), "parameter is not boolean"
    return value.to_bytes(1, byteorder='big')


def none2bytes(value):
    assert value is None, "parameter is not None"
    value = 0  # simulate None
    return value.to_bytes(1, byteorder='big')


def listdir_nohidden(dirpath):
    return glob.glob(os.path.join(dirpath, '*'))


def write_sync(file):
    file.write(SYNC_ESCAPE.to_bytes(2, byteorder='big', signed=True))
    file.write(SYNC_MARK)

def write_fileheader(file):
    file.write(b'SEQ1')  # Version
    file.write(b'TEXT')  # Key Class Name
    file.write(b'BYTEARRAY')  # Value Class Name
    file.write(boolean2bytes(False))  # Is Compressed
    file.write(boolean2bytes(False))  # Is Block Compressed
    file.write(none2bytes(None))  # Compression Codec
    file.write(none2bytes(None))  # Metadata a Treemap of key/value pairs
    write_sync(file)


def write_uncompressed(file):
    parents = listdir_nohidden(SAMPLE_PATH)
    p = 0
    for par in parents:
        key = os.path.basename(par)
        images = listdir_nohidden(par)
        i = 0
        for image in images:
            key = key + '-' + os.path.basename(image).split('.')[0]
            keylength = len(key)
            ds = dicom.read_file(image)
            value = ds.PixelData
            reclength = keylength + len(value)
            file.write(reclength.to_bytes(4,byteorder='big'))  # Record Length
            file.write(keylength.to_bytes(2,byteorder='big'))  # Key Length
            file.write(key.encode())
            file.write(value)
            write_sync(file)
            i += 1
            if i > 2:
                break  # Inner LOOP
        p += 1
        if p > 2:
            break  # Outer LOOP


def main(argv):
    global SAMPLE_PATH

    try:
        opts, args = getopt.getopt(argv, 'h')
    except getopt.GetoptError:
        print(HELP_MSG)
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print(HELP_MSG)
            sys.exit()

    SAMPLE_PATH = args[0] if (type(args) in (tuple, list)) else args

    with open(OUTPUT_FILENAME, 'wb') as out:
        write_fileheader(out)
        write_uncompressed(out)


if __name__ == "__main__":
    main(sys.argv[1])
