/*
 * Ceaflate
 *
 * Copyright (c) Kavawuvi 2020. This software is released under GPL version 3. See COPYING for more information.
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <vector>
#include <mutex>
#include <climits>
#include <thread>
#include <zlib.h>

static void exit_usage(const char **argv);
static int compress_file(const char *input_file, const char *output_file);
static int decompress_file(const char *input_file, const char *output_file);
static std::vector<std::byte> read_file(const char *input, std::size_t minimum_size);

#define NOTE "(')> "
#define SUCCESS "(^)< "
#define ERROR "(X)> "

int main(int argc, const char **argv) {
    // Make sure we have enough arguments!
    if(argc != 4) {
        exit_usage(argv);
    }

    // Do the compress!
    if(std::strcmp(argv[1], "c") == 0) {
        return compress_file(argv[2], argv[3]);
    }
    else if(std::strcmp(argv[1], "d") == 0) {
        return decompress_file(argv[2], argv[3]);
    }
    else {
        exit_usage(argv);
    }
}

#define CHUNK_SIZE static_cast<std::size_t>(0x20000)

struct CompressedMapHeader {
    static const constexpr std::size_t MAX_BLOCKS = 0xFFFF;
    std::uint32_t block_count;
    std::uint32_t block_offsets[MAX_BLOCKS];
};

struct Worker {
    const std::byte *input = nullptr;
    std::size_t input_size = 0;
    std::unique_ptr<std::byte []> output;
    std::size_t output_size = 0;
    std::size_t offset = 0;
    bool failure = false;
    std::mutex mutex;
    bool started = false;
};

static void perform_decompression(Worker *worker);
static void perform_compression(Worker *worker);
static void perform_job(std::vector<Worker> &workers, void (*function)(Worker *));
static int write_file(const char *output_file, const std::vector<Worker> &workers, const std::vector<std::byte> &start = std::vector<std::byte>());

static int decompress_file(const char *input_file, const char *output_file) {
    // Read the file
    auto compressed_file = read_file(input_file, sizeof(CompressedMapHeader));
    auto file_size = compressed_file.size();
    const auto &header = *reinterpret_cast<const CompressedMapHeader *>(compressed_file.data());

    // Make sure it's not bullshit
    auto block_count = static_cast<std::size_t>(header.block_count);
    if(header.block_count > CompressedMapHeader::MAX_BLOCKS) {
        std::fprintf(stderr, ERROR "Invalid block count (%zu > %zu)\n", block_count, CompressedMapHeader::MAX_BLOCKS);
        return EXIT_FAILURE;
    }
    else if(header.block_count == 0) {
        std::fprintf(stderr, ERROR "Invalid block count (%zu == 0)\n", block_count);
        return EXIT_FAILURE;
    }
    std::printf(NOTE "Decompressing %zu chunk%s...\n", block_count, block_count == 1 ? "" : "s");

    // Allocate workers
    std::vector<Worker> workers(block_count);
    for(std::size_t i = 0; i < block_count; i++) {
        std::size_t offset = header.block_offsets[i];
        if(offset + sizeof(std::uint32_t) > file_size) {
            std::fprintf(stderr, ERROR "Block #%zu has an invalid offset (%zu + %zu > %zu)\n", i, offset, sizeof(std::uint32_t), file_size);
            return EXIT_FAILURE;
        }

        // Set up the worker
        auto *input = compressed_file.data() + offset;
        auto &uncompressed_size = *reinterpret_cast<std::uint32_t *>(input);
        auto remaining_size = file_size - offset + sizeof(uncompressed_size);
        workers[i].input = input + sizeof(uncompressed_size);
        workers[i].input_size = remaining_size;
        workers[i].output = std::make_unique<std::byte []>(uncompressed_size);
        workers[i].output_size = uncompressed_size;
    }

    // Do it!
    perform_job(workers, perform_decompression);

    // If we failed, error out
    bool failed = false;
    for(std::size_t i = 0; i < block_count; i++) {
        if(workers[i].failure) {
            std::fprintf(stderr, ERROR "Block #%zu failed to decompress\n", i);
            failed = true;
        }
    }
    if(failed) {
        return EXIT_FAILURE;
    }

    return write_file(output_file, workers);
}

#define CHUNK_SIZE static_cast<std::size_t>(0x20000)

struct CacheFileHeader {
    std::uint32_t head_literal;
    std::uint32_t engine;
    std::uint32_t decompressed_file_size;
    std::uint8_t pad1[0x4];
    std::uint32_t tag_data_offset;
    std::uint32_t tag_data_size;
    std::uint8_t pad2[0x8];
    char name[0x20];
    char build[0x20];
    std::uint16_t map_type;
    std::byte pad3[0x2];
    std::uint32_t crc32;
    std::byte pad4[0x794];
    std::uint32_t foot_literal;
};
static_assert(sizeof(CacheFileHeader) == 0x800);

static int compress_file(const char *input_file, const char *output_file) {
    // Read the file
    auto uncompressed_file = read_file(input_file, 0);
    auto file_size = uncompressed_file.size();
    std::size_t block_count = file_size / CHUNK_SIZE + ((file_size % CHUNK_SIZE) > 0);

    // Make a header
    std::vector<std::byte> header(sizeof(CompressedMapHeader));
    CompressedMapHeader &header_v = *reinterpret_cast<CompressedMapHeader *>(header.data());
    if(block_count > CompressedMapHeader::MAX_BLOCKS) {
        std::fprintf(stderr, ERROR "Maximum blocks exceeded (#%zu > %zu)\n", block_count, CompressedMapHeader::MAX_BLOCKS);
        return EXIT_FAILURE;
    }
    header_v.block_count = static_cast<std::uint32_t>(block_count);
    std::printf(NOTE "Compressing %zu chunk%s...\n", block_count, block_count == 1 ? "" : "s");

    // Allocate workers
    std::vector<Worker> workers(block_count);
    for(std::size_t i = 0; i < block_count; i++) {
        std::size_t offset = i * CHUNK_SIZE;
        if(offset + sizeof(std::uint32_t) > file_size) {
            std::fprintf(stderr, ERROR "Block #%zu has an invalid offset (%zu + %zu > %zu)\n", i, offset, sizeof(std::uint32_t), file_size);
            return EXIT_FAILURE;
        }

        // Set up the worker
        auto *input = uncompressed_file.data() + offset;
        std::size_t remaining_size = file_size - offset;
        if(remaining_size > CHUNK_SIZE) {
            remaining_size = CHUNK_SIZE;
        }
        workers[i].offset = sizeof(std::uint32_t);
        std::size_t max_size = remaining_size * 2 + workers[i].offset;

        workers[i].input = input;
        workers[i].input_size = remaining_size;
        workers[i].output = std::make_unique<std::byte []>(max_size);
        workers[i].output_size = max_size;

        *reinterpret_cast<std::uint32_t *>(workers[i].output.get()) = static_cast<std::uint32_t>(remaining_size);
    }

    // Do it!
    perform_job(workers, perform_compression);

    // If we failed, error out
    bool failed = false;
    for(std::size_t i = 0; i < block_count; i++) {
        if(workers[i].failure) {
            std::fprintf(stderr, ERROR "Block #%zu failed to compress\n", i);
            failed = true;
        }
    }
    if(failed) {
        return EXIT_FAILURE;
    }

    // Set the offsets
    std::size_t current_offset = header.size();
    for(std::size_t i = 0; i < block_count; i++) {
        if(current_offset > UINT32_MAX) {
            std::fprintf(stderr, ERROR "Final size exceeds the maximum limit (%zu > %zu)\n", current_offset, static_cast<std::size_t>(UINT32_MAX));
            return EXIT_FAILURE;
        }
        header_v.block_offsets[i] = static_cast<std::uint32_t>(current_offset);
        current_offset += workers[i].output_size;
    }

    return write_file(output_file, workers, header);
}

static void perform_decompression(Worker *worker) {
    z_stream inflate_stream = {};
    inflate_stream.zalloc = Z_NULL;
    inflate_stream.zfree = Z_NULL;
    inflate_stream.opaque = Z_NULL;
    inflate_stream.avail_in = worker->input_size;
    inflate_stream.next_in = const_cast<Bytef *>(reinterpret_cast<const Bytef *>(worker->input));
    inflate_stream.avail_out = worker->output_size;
    inflate_stream.next_out = reinterpret_cast<Bytef *>(worker->output.get());
    if((inflateInit(&inflate_stream) != Z_OK) || (inflate(&inflate_stream, Z_FINISH) != Z_STREAM_END) || (inflateEnd(&inflate_stream) != Z_OK)) {
        worker->failure = true;
    }
    worker->mutex.unlock();
}

static void perform_compression(Worker *worker) {
    z_stream deflate_stream = {};
    deflate_stream.zalloc = Z_NULL;
    deflate_stream.zfree = Z_NULL;
    deflate_stream.opaque = Z_NULL;
    deflate_stream.avail_in = worker->input_size;
    deflate_stream.next_in = const_cast<Bytef *>(reinterpret_cast<const Bytef *>(worker->input));
    deflate_stream.avail_out = worker->output_size - sizeof(std::uint32_t);
    deflate_stream.next_out = reinterpret_cast<Bytef *>(worker->output.get()) + sizeof(std::uint32_t);
    if((deflateInit(&deflate_stream, Z_BEST_COMPRESSION) != Z_OK) || (deflate(&deflate_stream, Z_FINISH) != Z_STREAM_END) || (deflateEnd(&deflate_stream) != Z_OK)) {
        worker->failure = true;
    }
    else {
        worker->output_size = deflate_stream.total_out + sizeof(std::uint32_t);
    }
    worker->mutex.unlock();
}

static void exit_usage(const char **argv) {
    std::printf(NOTE "Usage: %s <c|d> <input> <output>\n", *argv);
    std::exit(EXIT_FAILURE);
}

static std::vector<std::byte> read_file(const char *input, std::size_t minimum_size) {
    #ifdef _WIN32
    #define TELL _ftelli64
    #define SEEK _fseeki64
    #else
    #define TELL std::ftell
    #define SEEK std::fseek
    #endif

    // Open the file
    auto *file = std::fopen(input, "rb");
    if(!file) {
        std::fprintf(stderr, ERROR "Failed to open %s for reading\n", input);
        std::exit(EXIT_FAILURE);
    }

    // Start to read it. Make sure we have enough space
    std::vector<std::byte> data(minimum_size);

    // Make sure we can read everything
    if(minimum_size && std::fread(data.data(), minimum_size, 1, file) != 1) {
        std::fprintf(stderr, ERROR "%s doesn't have enough bytes to be valid\n", input);
        std::fclose(file);
        std::exit(EXIT_FAILURE);
    }

    // Read everything
    std::size_t offset = minimum_size;
    std::fseek(file, 0, SEEK_END);
    std::size_t size = static_cast<std::size_t>(TELL(file));
    data.resize(size, std::byte());
    SEEK(file, offset, SEEK_SET);
    while(offset < size) {
        std::size_t remainder = size - offset;
        if(std::fread(data.data() + offset, remainder > LONG_MAX ? LONG_MAX : static_cast<long>(remainder), 1, file) != 1) {
            std::fprintf(stderr, ERROR "An error occurred when reading %s\n", input);
            std::fclose(file);
            std::exit(EXIT_FAILURE);
        }
        offset += remainder;
    }
    std::fclose(file);

    return data;
}

static int write_file(const char *output_file, const std::vector<Worker> &workers, const std::vector<std::byte> &start) {
    std::FILE *f = std::fopen(output_file, "wb");
    if(!f) {
        std::fprintf(stderr, ERROR "Failed to open %s for writing\n", output_file);
        return EXIT_FAILURE;
    }

    // First write start() in case we're writing a header
    if(start.size()) {
        if(std::fwrite(start.data(), start.size(), 1, f) == 0) {
            std::fprintf(stderr, ERROR "Failed to write to %s\n", output_file);
            std::fclose(f);
            return EXIT_FAILURE;
        }
    }

    for(auto &w : workers) {
        if(std::fwrite(w.output.get(), w.output_size, 1, f) == 0) {
            std::fprintf(stderr, ERROR "Failed to write to %s\n", output_file);
            std::fclose(f);
            return EXIT_FAILURE;
        }
    }
    std::fclose(f);

    std::printf(SUCCESS "Done!\n");
    return EXIT_SUCCESS;
}

static std::size_t busy_workers(std::vector<Worker> &workers);
static Worker *next_worker(std::vector<Worker> &workers);

static void perform_job(std::vector<Worker> &workers, void (*function)(Worker *)) {
    if(workers.size() == 0) {
        return;
    }

    // Lock the mutex!
    for(auto &w : workers) {
        w.mutex.lock();
    }

    // If we aren't threaded, just do a for loop
    std::size_t max_threads = std::thread::hardware_concurrency();
    if(max_threads <= 1) {
        for(auto &worker : workers) {
            function(&worker);
        }
    }

    // Otherwise, do it!
    else {
        while(true) {
            auto *next = next_worker(workers);
            auto busy_count = busy_workers(workers);
            if(busy_count == 0 && !next) {
                break;
            }
            else if(next && busy_count < max_threads) {
                next->started = true;
                std::thread(function, next).detach();
            }
            else {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    }
}

static std::size_t busy_workers(std::vector<Worker> &workers) {
    std::size_t count = 0;
    for(auto &w : workers) {
        if(w.mutex.try_lock()) {
            w.mutex.unlock();
        }
        else {
            count += w.started;
        }
    }
    return count;
}

static Worker *next_worker(std::vector<Worker> &workers) {
    for(auto &w : workers) {
        if(!w.started) {
            return &w;
        }
    }
    return nullptr;
}
