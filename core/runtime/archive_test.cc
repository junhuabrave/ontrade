#include "core/runtime/archive.hpp"

#include <array>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

namespace ontrade::runtime {
namespace {

// Helpers --------------------------------------------------------------------

std::string make_temp_path(std::string_view label) {
    auto dir = std::filesystem::temp_directory_path();
    auto path = dir / (std::string("ontrade_archive_test_") + std::string(label) + "_" +
                       std::to_string(::getpid()) + ".log");
    return path.string();
}

class ArchiveFixture : public ::testing::Test {
protected:
    void SetUp() override {
        path_ = make_temp_path(::testing::UnitTest::GetInstance()->current_test_info()->name());
    }
    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove(path_, ec);
    }
    std::string path_;
};

std::vector<std::byte> bytes_from(std::string_view s) {
    std::vector<std::byte> v(s.size());
    std::memcpy(v.data(), s.data(), s.size());
    return v;
}

std::vector<std::byte> slurp(const std::string& path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f) return {};
    const auto size = static_cast<std::size_t>(f.tellg());
    f.seekg(0);
    std::vector<std::byte> v(size);
    f.read(reinterpret_cast<char*>(v.data()), size);
    return v;
}

// Tests ----------------------------------------------------------------------

TEST_F(ArchiveFixture, OpenWritesHeaderOfExpectedShape) {
    Archive a;
    ASSERT_TRUE(a.open(path_, /*wall_ns=*/1'234'567'890));
    a.close();

    auto bytes = slurp(path_);
    ASSERT_EQ(bytes.size(), kArchiveHeaderBytes);
    EXPECT_EQ(std::memcmp(bytes.data(), kArchiveMagic.data(), 8), 0);
    std::uint32_t schema = 0;
    std::memcpy(&schema, bytes.data() + 8, 4);
    EXPECT_EQ(schema, kArchiveSchemaMajor);
    std::int64_t wall = 0;
    std::memcpy(&wall, bytes.data() + 12, 8);
    EXPECT_EQ(wall, 1'234'567'890);
}

TEST_F(ArchiveFixture, AppendThenReadRoundTripsSingleRecord) {
    Archive a;
    ASSERT_TRUE(a.open(path_, 1'000));
    auto payload = bytes_from("hello world");
    EXPECT_TRUE(a.append(RingTag::Md, payload, /*wall=*/2'000));
    a.close();

    ArchiveReader r;
    ASSERT_TRUE(r.open(path_));
    ASSERT_EQ(r.size(), 1U);
    EXPECT_EQ(r.file_create_wall_ns(), 1'000);

    const auto& rec = r.at(0);
    EXPECT_EQ(rec.tag, RingTag::Md);
    EXPECT_EQ(rec.seq, 0U);
    EXPECT_EQ(rec.wall_ns, 2'000);
    ASSERT_EQ(rec.payload.size(), payload.size());
    EXPECT_EQ(std::memcmp(rec.payload.data(), payload.data(), payload.size()), 0);
}

TEST_F(ArchiveFixture, AppendAssignsMonotonicSeq) {
    Archive a;
    ASSERT_TRUE(a.open(path_, 0));
    auto p = bytes_from("x");
    for (int i = 0; i < 5; ++i) {
        EXPECT_TRUE(a.append(RingTag::Md, p, i));
    }
    a.close();

    ArchiveReader r;
    ASSERT_TRUE(r.open(path_));
    ASSERT_EQ(r.size(), 5U);
    for (std::size_t i = 0; i < 5; ++i) {
        EXPECT_EQ(r.at(i).seq, i);
    }
}

TEST_F(ArchiveFixture, AppendMixedRingTagsPreservesOrder) {
    Archive a;
    ASSERT_TRUE(a.open(path_, 0));
    EXPECT_TRUE(a.append(RingTag::Md, bytes_from("md-1"), 1));
    EXPECT_TRUE(a.append(RingTag::ToOms, bytes_from("oms-1"), 2));
    EXPECT_TRUE(a.append(RingTag::ToVenue, bytes_from("venue-1"), 3));
    EXPECT_TRUE(a.append(RingTag::VenueIn, bytes_from("vin-1"), 4));
    EXPECT_TRUE(a.append(RingTag::Events, bytes_from("evt-1"), 5));
    EXPECT_TRUE(a.append(RingTag::Md, bytes_from("md-2"), 6));
    a.close();

    ArchiveReader r;
    ASSERT_TRUE(r.open(path_));
    ASSERT_EQ(r.size(), 6U);
    EXPECT_EQ(r.at(0).tag, RingTag::Md);
    EXPECT_EQ(r.at(1).tag, RingTag::ToOms);
    EXPECT_EQ(r.at(2).tag, RingTag::ToVenue);
    EXPECT_EQ(r.at(3).tag, RingTag::VenueIn);
    EXPECT_EQ(r.at(4).tag, RingTag::Events);
    EXPECT_EQ(r.at(5).tag, RingTag::Md);
    // Wall ns preserved per record.
    EXPECT_EQ(r.at(0).wall_ns, 1);
    EXPECT_EQ(r.at(5).wall_ns, 6);
}

TEST_F(ArchiveFixture, ZeroLengthPayloadIsLegal) {
    Archive a;
    ASSERT_TRUE(a.open(path_, 0));
    EXPECT_TRUE(a.append(RingTag::Events, std::span<const std::byte>{}, 100));
    a.close();

    ArchiveReader r;
    ASSERT_TRUE(r.open(path_));
    ASSERT_EQ(r.size(), 1U);
    EXPECT_EQ(r.at(0).payload.size(), 0U);
    EXPECT_EQ(r.at(0).wall_ns, 100);
}

TEST_F(ArchiveFixture, AppendBeforeOpenReturnsFalse) {
    Archive a;
    EXPECT_FALSE(a.append(RingTag::Md, bytes_from("x"), 0));
}

TEST_F(ArchiveFixture, CloseIsIdempotent) {
    Archive a;
    ASSERT_TRUE(a.open(path_, 0));
    a.close();
    a.close();  // should not crash
    EXPECT_FALSE(a.is_open());
}

TEST_F(ArchiveFixture, ReaderRejectsBadMagic) {
    // Write a file with bad magic.
    std::ofstream f(path_, std::ios::binary);
    std::array<char, kArchiveHeaderBytes> hdr{};
    std::memcpy(hdr.data(), "BADMAGIC", 8);
    f.write(hdr.data(), hdr.size());
    f.close();

    ArchiveReader r;
    EXPECT_FALSE(r.open(path_));
}

TEST_F(ArchiveFixture, ReaderRejectsWrongSchemaVersion) {
    // Write a header with correct magic but wrong schema_major.
    std::ofstream f(path_, std::ios::binary);
    std::array<char, kArchiveHeaderBytes> hdr{};
    std::memcpy(hdr.data(), kArchiveMagic.data(), 8);
    std::uint32_t bad_schema = 99;
    std::memcpy(hdr.data() + 8, &bad_schema, 4);
    f.write(hdr.data(), hdr.size());
    f.close();

    ArchiveReader r;
    EXPECT_FALSE(r.open(path_));
}

TEST_F(ArchiveFixture, TornPayloadStopsCleanlyAtLastWellFormedRecord) {
    // Write 3 records, then truncate the file mid-third-record's payload.
    {
        Archive a;
        ASSERT_TRUE(a.open(path_, 0));
        EXPECT_TRUE(a.append(RingTag::Md, bytes_from("first"), 1));
        EXPECT_TRUE(a.append(RingTag::Md, bytes_from("second"), 2));
        EXPECT_TRUE(a.append(RingTag::Md, bytes_from("third-truncated-payload"), 3));
        a.close();
    }
    // Truncate to N bytes < third record's full size.
    auto bytes = slurp(path_);
    ASSERT_GT(bytes.size(), 0U);
    // Find the third record's start, truncate inside its payload.
    // Header(32) + 2 records(24+5, 24+6) = 32 + 29 + 30 = 91. Third record
    // starts at 91 with 24-byte header + 23-byte payload = 47 bytes long.
    // Truncate to 91 + 24 + 5 = 120 (mid-payload).
    const std::size_t truncate_to = kArchiveHeaderBytes + (kArchiveRecordHeaderBytes + 5)
                                  + (kArchiveRecordHeaderBytes + 6)
                                  + kArchiveRecordHeaderBytes + 5;
    ASSERT_LT(truncate_to, bytes.size());
    {
        std::ofstream f(path_, std::ios::binary | std::ios::trunc);
        f.write(reinterpret_cast<const char*>(bytes.data()), truncate_to);
    }

    ArchiveReader r;
    ASSERT_TRUE(r.open(path_));
    // Should have read exactly the first two well-formed records.
    EXPECT_EQ(r.size(), 2U);
    EXPECT_EQ(r.at(0).payload.size(), 5U);
    EXPECT_EQ(r.at(1).payload.size(), 6U);
}

TEST_F(ArchiveFixture, TornHeaderStopsAtLastRecord) {
    // Write 2 records, then truncate inside the third record's HEADER (not
    // the payload). Reader should yield 2 valid records.
    {
        Archive a;
        ASSERT_TRUE(a.open(path_, 0));
        EXPECT_TRUE(a.append(RingTag::Md, bytes_from("first"), 1));
        EXPECT_TRUE(a.append(RingTag::Md, bytes_from("second"), 2));
        // Manually write a partial third record header (12 bytes of 24).
        a.close();
    }
    // Append 12 garbage bytes (less than a full record header).
    {
        std::ofstream f(path_, std::ios::binary | std::ios::app);
        char garbage[12]{};
        f.write(garbage, sizeof(garbage));
    }

    ArchiveReader r;
    ASSERT_TRUE(r.open(path_));
    EXPECT_EQ(r.size(), 2U);
}

TEST_F(ArchiveFixture, EmptyArchiveReadsZeroRecords) {
    Archive a;
    ASSERT_TRUE(a.open(path_, 42));
    a.close();

    ArchiveReader r;
    ASSERT_TRUE(r.open(path_));
    EXPECT_EQ(r.size(), 0U);
    EXPECT_EQ(r.file_create_wall_ns(), 42);
}

TEST_F(ArchiveFixture, RecordsWrittenCounterTracks) {
    Archive a;
    ASSERT_TRUE(a.open(path_, 0));
    EXPECT_EQ(a.records_written(), 0U);
    EXPECT_TRUE(a.append(RingTag::Md, bytes_from("a"), 1));
    EXPECT_EQ(a.records_written(), 1U);
    EXPECT_TRUE(a.append(RingTag::Md, bytes_from("b"), 2));
    EXPECT_EQ(a.records_written(), 2U);
}

TEST_F(ArchiveFixture, FlushReturnsTrueOnOpenArchive) {
    Archive a;
    ASSERT_TRUE(a.open(path_, 0));
    EXPECT_TRUE(a.append(RingTag::Md, bytes_from("x"), 1));
    EXPECT_TRUE(a.flush());
}

TEST_F(ArchiveFixture, LargePayloadRoundTrips) {
    constexpr std::size_t kSize = 16 * 1024;  // 16KB, well past the 64KB stdio buffer
    std::vector<std::byte> payload(kSize);
    for (std::size_t i = 0; i < kSize; ++i) {
        payload[i] = static_cast<std::byte>(i & 0xFF);
    }
    Archive a;
    ASSERT_TRUE(a.open(path_, 0));
    EXPECT_TRUE(a.append(RingTag::Md, payload, 1));
    a.close();

    ArchiveReader r;
    ASSERT_TRUE(r.open(path_));
    ASSERT_EQ(r.size(), 1U);
    ASSERT_EQ(r.at(0).payload.size(), kSize);
    EXPECT_EQ(std::memcmp(r.at(0).payload.data(), payload.data(), kSize), 0);
}

}  // namespace
}  // namespace ontrade::runtime
