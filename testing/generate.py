import random, string, sys

def randomword(length):
    letters = string.ascii_lowercase
    return random.choice(letters + string.digits) * length

def create_random_string_file(filepath, file_size):
    chunk_size = 1024 * 1024
    with open(filepath, 'w', encoding='ascii') as f:
        for i in range(file_size):
            print(f'epoch {i}')
            random_str = randomword(chunk_size)
            f.write(random_str)
            f.flush()

if __name__ == '__main__':
    filesize = int(sys.argv[1])
    filepath = f'../client/data/{filesize}_mb.txt'
    create_random_string_file(filepath, filesize)
