#pragma once

// Event archive — write-side and read-side for the binary log defined in
// docs/adr/003-archive-format.md.
//
// Producers (OMS, Gateway, StrategyRunner) call append() immediately after
// committing a message to a ring; the orchestrator owns the Archive object
// and passes a non-owning pointer to each component. With the pointer null
// the archive is a no-op — tests and the smoke binary use that to keep the
// archive optional during MVP work.
//
// File format (recap from ADR-003):
//
//   Header (32 bytes):
//     [0..7]   magic "ONTRADE\0"
//     [8..11]  schema_major (uint32, currently 1)
//     [12..19] file_create_wall_ns (int64)
//     [20..31] reserved (zero)
//
//   Record (24-byte header + variable payload):
//     [0..3]   total_length (uint32, includes header + payload)
//     [4]      ring_tag (uint8 — RingTag enum)
//     [5..7]   reserved (zero)
//     [8..15]  seq (uint64, monotonic per archive)
//     [16..23] wall_ns (int64, capture time)
//     [24..N]  payload bytes (ring slot verbatim)
//
// Threading: single-writer by contract. The orchestrator's poll loop is
// single-threaded; the Archive lives in that thread. No locks, no atomics
// on the append path.

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace ontrade::runtime {

inline constexpr std::array<char, 8> kArchiveMagic = {'O', 'N', 'T', 'R', 'A', 'D', 'E', '\0'};
inline constexpr std::uint32_t kArchiveSchemaMajor = 1;
inline constexpr std::size_t kArchiveHeaderBytes = 32;
inline constexpr std::size_t kArchiveRecordHeaderBytes = 24;

// Tag identifying which ring a record came from. Sized at uint8_t so an
// expanded ring topology in the future does not bump record overhead.
enum class RingTag : std::uint8_t {
    Unknown = 0,
    Md = 1,
    ToOms = 2,
    ToVenue = 3,
    VenueIn = 4,
    Events = 5,
};

// Write side. Owns a stdio FILE* with a 64 KB fully-buffered output buffer
// so append() amortizes the syscall over many records.
class Archive {
public:
    // Open a new archive file at `path`, truncating any existing content.
    // Writes the 32-byte header. Returns true on success; false if the file
    // could not be opened or the header write failed.
    [[nodiscard]] bool open(std::string_view path, std::int64_t file_create_wall_ns) noexcept {
        close_internal();
        // fopen needs a NUL-terminated string; copy.
        std::string p(path);
        fp_ = std::fopen(p.c_str(), "wb");
        if (fp_ == nullptr) {
            return false;
        }
        // Fully buffered 64 KB output. setvbuf returns 0 on success.
        if (std::setvbuf(fp_, buffer_, _IOFBF, sizeof(buffer_)) != 0) {
            close_internal();
            return false;
        }

        std::byte hdr[kArchiveHeaderBytes]{};
        std::memcpy(hdr + 0, kArchiveMagic.data(), 8);
        const std::uint32_t schema = kArchiveSchemaMajor;
        std::memcpy(hdr + 8, &schema, 4);
        std::memcpy(hdr + 12, &file_create_wall_ns, 8);
        // bytes 20..31 already zero (reserved)

        if (std::fwrite(hdr, 1, kArchiveHeaderBytes, fp_) != kArchiveHeaderBytes) {
            close_internal();
            return false;
        }
        next_seq_ = 0;
        records_written_ = 0;
        return true;
    }

    // Append a single record. payload is the raw message bytes from the ring
    // slot (typically sizeof(proto::hot::OrderNew) or similar). Returns true
    // on success, false on any I/O error (file is left open in error state;
    // the caller decides whether to close + reopen or halt).
    [[nodiscard]] bool append(RingTag tag, std::span<const std::byte> payload,
                              std::int64_t wall_ns) noexcept {
        if (fp_ == nullptr) {
            return false;
        }
        const auto payload_bytes = payload.size();
        // Guard against record overflow of uint32 length field.
        if (payload_bytes > kMaxPayloadBytes) {
            return false;
        }
        const auto total = static_cast<std::uint32_t>(kArchiveRecordHeaderBytes + payload_bytes);

        std::byte rec_hdr[kArchiveRecordHeaderBytes]{};
        std::memcpy(rec_hdr + 0, &total, 4);
        rec_hdr[4] = static_cast<std::byte>(tag);
        // bytes 5..7 reserved (zero)
        const auto seq = next_seq_++;
        std::memcpy(rec_hdr + 8, &seq, 8);
        std::memcpy(rec_hdr + 16, &wall_ns, 8);

        if (std::fwrite(rec_hdr, 1, kArchiveRecordHeaderBytes, fp_) != kArchiveRecordHeaderBytes) {
            return false;
        }
        if (payload_bytes > 0 &&
            std::fwrite(payload.data(), 1, payload_bytes, fp_) != payload_bytes) {
            return false;
        }
        ++records_written_;
        return true;
    }

    // Force the userspace buffer to the kernel. Does NOT fsync — the kernel
    // is free to cache. Call before reading the file from another process or
    // before a clean shutdown if durability matters more than latency.
    [[nodiscard]] bool flush() noexcept {
        if (fp_ == nullptr) {
            return false;
        }
        return std::fflush(fp_) == 0;
    }

    // Flush + close. Idempotent.
    void close() noexcept { close_internal(); }

    [[nodiscard]] bool is_open() const noexcept { return fp_ != nullptr; }
    [[nodiscard]] std::uint64_t records_written() const noexcept { return records_written_; }
    [[nodiscard]] std::uint64_t next_seq() const noexcept { return next_seq_; }

    Archive() = default;
    ~Archive() { close_internal(); }
    Archive(const Archive&) = delete;
    Archive& operator=(const Archive&) = delete;
    Archive(Archive&&) = delete;
    Archive& operator=(Archive&&) = delete;

private:
    static constexpr std::size_t kMaxPayloadBytes =
        std::numeric_limits<std::uint32_t>::max() - kArchiveRecordHeaderBytes;

    void close_internal() noexcept {
        if (fp_ != nullptr) {
            std::fflush(fp_);
            std::fclose(fp_);
            fp_ = nullptr;
        }
    }

    std::FILE* fp_{nullptr};
    char buffer_[64 * 1024]{};
    std::uint64_t next_seq_{0};
    std::uint64_t records_written_{0};
};

// Read side. Loads the file fully into memory (archives are bounded by
// trading-day size; even a busy day fits comfortably in RAM). The replay
// tool will use this; tests use it for round-trip verification.
class ArchiveReader {
public:
    struct Record {
        RingTag tag;
        std::uint64_t seq;
        std::int64_t wall_ns;
        std::span<const std::byte> payload;
    };

    // Open and load a file. Returns true on success; false on I/O error,
    // bad magic, or schema mismatch.
    [[nodiscard]] bool open(std::string_view path) noexcept {
        std::string p(path);
        std::FILE* fp = std::fopen(p.c_str(), "rb");
        if (fp == nullptr) {
            return false;
        }
        // Read entire file. Archive sizes are bounded; production use will
        // stream record-by-record, but for the MVP we load eagerly.
        if (std::fseek(fp, 0, SEEK_END) != 0) {
            std::fclose(fp);
            return false;
        }
        const auto end = std::ftell(fp);
        if (end < 0) {
            std::fclose(fp);
            return false;
        }
        if (std::fseek(fp, 0, SEEK_SET) != 0) {
            std::fclose(fp);
            return false;
        }
        const auto size = static_cast<std::size_t>(end);
        bytes_.resize(size);
        if (size > 0) {
            const auto got = std::fread(bytes_.data(), 1, size, fp);
            if (got != size) {
                std::fclose(fp);
                bytes_.clear();
                return false;
            }
        }
        std::fclose(fp);

        // Validate header.
        if (bytes_.size() < kArchiveHeaderBytes) {
            return false;
        }
        if (std::memcmp(bytes_.data(), kArchiveMagic.data(), 8) != 0) {
            return false;
        }
        std::uint32_t schema = 0;
        std::memcpy(&schema, bytes_.data() + 8, 4);
        if (schema != kArchiveSchemaMajor) {
            return false;
        }
        std::memcpy(&file_create_wall_ns_, bytes_.data() + 12, 8);

        // Walk records. Stop at first malformed record (torn-tail tolerance).
        records_.clear();
        std::size_t pos = kArchiveHeaderBytes;
        while (pos < bytes_.size()) {
            if (bytes_.size() - pos < kArchiveRecordHeaderBytes) {
                break;  // torn header
            }
            std::uint32_t total = 0;
            std::memcpy(&total, bytes_.data() + pos, 4);
            if (total < kArchiveRecordHeaderBytes) {
                break;  // corrupt total
            }
            if (bytes_.size() - pos < total) {
                break;  // torn payload
            }
            Record r{};
            r.tag = static_cast<RingTag>(static_cast<std::uint8_t>(bytes_[pos + 4]));
            std::memcpy(&r.seq, bytes_.data() + pos + 8, 8);
            std::memcpy(&r.wall_ns, bytes_.data() + pos + 16, 8);
            r.payload = std::span<const std::byte>(
                bytes_.data() + pos + kArchiveRecordHeaderBytes,
                total - kArchiveRecordHeaderBytes);
            records_.push_back(r);
            pos += total;
        }
        return true;
    }

    [[nodiscard]] std::int64_t file_create_wall_ns() const noexcept { return file_create_wall_ns_; }
    [[nodiscard]] std::size_t size() const noexcept { return records_.size(); }
    [[nodiscard]] const Record& at(std::size_t i) const noexcept { return records_[i]; }
    [[nodiscard]] std::span<const Record> records() const noexcept {
        return std::span<const Record>(records_.data(), records_.size());
    }

private:
    std::vector<std::byte> bytes_;
    std::vector<Record> records_;
    std::int64_t file_create_wall_ns_{0};
};

}  // namespace ontrade::runtime
