import pyesiaf


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
    formats = input_formats + output_formats
    # TODO proper implementation
    return formats[0]
