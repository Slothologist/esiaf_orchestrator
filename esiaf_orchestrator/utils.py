import pyesiaf
import sys

BITRATE_DICT = {
    pyesiaf.Bitrate.BIT_INT_8_SIGNED: 8,
    pyesiaf.Bitrate.BIT_INT_8_UNSIGNED: 8,
    pyesiaf.Bitrate.BIT_INT_16_SIGNED: 16,
    pyesiaf.Bitrate.BIT_INT_16_UNSIGNED: 16,
    pyesiaf.Bitrate.BIT_INT_24_SIGNED: 24,
    pyesiaf.Bitrate.BIT_INT_24_UNSIGNED: 24,
    pyesiaf.Bitrate.BIT_INT_32_SIGNED: 32,
    pyesiaf.Bitrate.BIT_INT_32_UNSIGNED: 32,
    pyesiaf.Bitrate.BIT_FLOAT_32: 32,
    pyesiaf.Bitrate.BIT_FLOAT_64: 64
}


def calculate_format_network_cost(format):
    return format.channels * BITRATE_DICT[format.bitrate] * format.rate


def best_format_traffic_min(input_formats, output_formats):
    formats = input_formats + output_formats
    rates = [calculate_format_network_cost(x) for x in formats]
    return formats[rates.index(min(rates))]


def best_format_cpu_min(input_formats, output_formats):
    formats = set(input_formats + output_formats)
    current_min_format = []
    current_resample_count = sys.maxsize

    def amount_resamples_needed(format, all_formats):
        return len([f for f in all_formats if f != format])

    for each in formats:
        resamples = amount_resamples_needed(each, input_formats + output_formats)
        if resamples < current_resample_count:
            current_min_format = [each]
            current_resample_count = resamples
        if resamples == current_resample_count:
            current_min_format.append(each)
    return best_format_traffic_min(current_min_format, [])
