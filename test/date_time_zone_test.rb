require_relative 'test_helper'
require 'date'

# A date32 is a count of days since 1970-01-01, so the date written must not depend on the time zone of the process.
class DateTimeZoneTest < Minitest::Test
  # Dates on both sides of 1970-01-01, where dividing seconds toward zero, and taking midnight in the local time zone,
  # each moved a date by a day.
  DATES = [
    Date.new(1950, 1, 12),
    Date.new(1969, 12, 31),
    Date.new(1970, 1, 1),
    Date.new(1970, 1, 2),
    Date.new(2026, 10, 20)
  ].freeze

  # Time zones west and east of UTC, whose midnight falls on another day in UTC.
  ZONES = %w[UTC America/New_York Europe/Berlin Asia/Tokyo Pacific/Honolulu].freeze

  def setup
    @test_file = File.join(Dir.tmpdir, "test_date_time_zone_#{Process.pid}.parquet")
    @zone = ENV['TZ']
  end

  def teardown
    ENV['TZ'] = @zone
    File.delete(@test_file) if File.exist?(@test_file)
  end

  def test_date32_text_is_the_same_day_in_every_time_zone
    ZONES.each do |zone|
      ENV['TZ'] = zone

      assert_equal DATES, write_and_read(DATES.map(&:iso8601), {name: 'date', type: :date32}), zone
    end
  end

  def test_date32_text_with_a_format_is_the_same_day_in_every_time_zone
    text = DATES.map { |date| date.strftime('%d/%m/%Y') }

    ZONES.each do |zone|
      ENV['TZ'] = zone

      assert_equal DATES, write_and_read(text, {name: 'date', type: :date32, format: '%d/%m/%Y'}), zone
    end
  end

  def test_date32_time_is_its_day_in_utc_before_1970_too
    times = DATES.map { |date| Time.utc(date.year, date.month, date.day, 12, 30) }

    assert_equal DATES, write_and_read(times, {name: 'date', type: :date32})
  end

  private

  def write_and_read(values, field)
    Parquet.write_rows(values.map { |value| [value] }, schema: {fields: [field]}, write_to: @test_file)
    dates = []
    Parquet.each_row(@test_file) { |row| dates << row['date'] }
    dates
  end
end
