def get_windows(data, timestamps, threshold, min_diff, max_win_size):
    all_windows = []
    i = 0
    while i < len(data):
        l = timestamps.iloc[i]
        if data.iloc[i] < threshold:
            i += 1
            continue

        while i < len(data) and data.iloc[i] > threshold:
            if timestamps.iloc[i] - l > max_win_size:
                break
            i += 1

        r = timestamps.iloc[i - 1]
        all_windows.append([l, r])
    if len(all_windows) == 0:
        return []
    compressed_windows = [all_windows[0]]
    for i in range(1, len(all_windows)):
        if (all_windows[i][0] - compressed_windows[-1][1]) / 1000 < min_diff:
            compressed_windows[-1][1] = all_windows[i][1]
        else:
            compressed_windows.append(all_windows[i])
    return compressed_windows


def search_lb(df, val):
    tl = -1
    tr = len(df)

    while tr - tl > 1:
        tm = (tl + tr) // 2
        if df.iloc[tm] > val:
            tr = tm
        else:
            tl = tm
    return tl


def search_rb(df, val):
    tl = -1
    tr = len(df)

    while tr - tl > 1:
        tm = (tl + tr) // 2
        if df.iloc[tm] > val:
            tr = tm
        else:
            tl = tm
    return tr


def bytes_to_MB(bytes):
    return bytes*1E-6