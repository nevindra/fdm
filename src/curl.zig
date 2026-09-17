//! What a person pastes: a URL, or the `curl` line a browser's "Copy as
//! cURL" writes — `curl 'https://…' -H 'cookie: …' -H 'referer: …'` with
//! `\` continuations — and out of either, one `download.Add`.
//!
//! Only what such a line carries is read: the URL, `-H`, `-b`, `-A`, `-e`,
//! `-u`, `-o`. Everything else curl accepts is skipped, a value-taking
//! option with its value, so that `--compressed` or `-X GET` does not turn
//! into a URL. A `--data` is skipped too: fdm sends `GET`, and a link that
//! needs a body is not a download.

const std = @import("std");
const download = @import("download.zig");

const Add = download.Add;

/// `line` is a URL or a `curl` command; the result is allocated with `gpa`
/// and freed with `Add.free`.
pub fn parse(gpa: std.mem.Allocator, line: []const u8) !Add {
    const trimmed = std.mem.trim(u8, line, " \t\r\n");
    if (!std.mem.startsWith(u8, trimmed, "curl") or (trimmed.len > 4 and !std.ascii.isWhitespace(trimmed[4]))) {
        return .{ .url = try gpa.dupe(u8, trimmed) };
    }

    var words: std.ArrayList([]const u8) = .empty;
    defer {
        for (words.items) |w| gpa.free(w);
        words.deinit(gpa);
    }
    try split(gpa, trimmed[4..], &words);

    var url: ?[]const u8 = null;
    var out: ?[]const u8 = null;
    var headers: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (headers.items) |h| gpa.free(h);
        headers.deinit(gpa);
    }
    errdefer if (url) |u| gpa.free(u);
    errdefer if (out) |o| gpa.free(o);

    var i: usize = 0;
    while (i < words.items.len) : (i += 1) {
        const w = words.items[i];
        if (w.len > 0 and w[0] == '-' and w.len > 1) {
            const takes_value = isOne(w, &value_options);
            const value: ?[]const u8 = if (takes_value and i + 1 < words.items.len) words.items[i + 1] else null;
            if (takes_value) i += 1;
            const v = value orelse continue;
            if (isOne(w, &.{ "-H", "--header" })) {
                try headers.append(gpa, try gpa.dupe(u8, v));
            } else if (isOne(w, &.{ "-b", "--cookie" })) {
                try headers.append(gpa, try std.fmt.allocPrint(gpa, "Cookie: {s}", .{v}));
            } else if (isOne(w, &.{ "-A", "--user-agent" })) {
                try headers.append(gpa, try std.fmt.allocPrint(gpa, "User-Agent: {s}", .{v}));
            } else if (isOne(w, &.{ "-e", "--referer" })) {
                try headers.append(gpa, try std.fmt.allocPrint(gpa, "Referer: {s}", .{v}));
            } else if (isOne(w, &.{ "-u", "--user" })) {
                const enc = std.base64.standard.Encoder;
                const b64 = try gpa.alloc(u8, enc.calcSize(v.len));
                defer gpa.free(b64);
                try headers.append(gpa, try std.fmt.allocPrint(gpa, "Authorization: Basic {s}", .{enc.encode(b64, v)}));
            } else if (isOne(w, &.{ "-o", "--output" })) {
                if (out) |o| gpa.free(o);
                out = try gpa.dupe(u8, v);
            } else if (std.mem.eql(u8, w, "--url")) {
                if (url) |u| gpa.free(u);
                url = try gpa.dupe(u8, v);
            }
        } else if (url == null) {
            url = try gpa.dupe(u8, w);
        }
    }
    return .{
        .url = url orelse return error.NoUrl,
        .headers = try headers.toOwnedSlice(gpa),
        .out = out,
    };
}

/// The curl options that take a value, so that the value is never read as
/// the URL. The ones fdm uses are here with the ones it skips.
const value_options = [_][]const u8{
    "-H",         "--header",      "-b",               "--cookie",     "-A",    "--user-agent", "-e",                "--referer",
    "-u",         "--user",        "-o",               "--output",     "--url", "-X",           "--request",         "-d",
    "-c",         "--cookie-jar",  "-x",               "--proxy",      "-m",    "--max-time",   "--connect-timeout", "--data",
    "--data-raw", "--data-binary", "--data-urlencode", "--data-ascii", "-F",    "--form",       "--retry",           "--range",
    "-r",         "--limit-rate",  "--cacert",         "--cert",       "--key", "-T",           "--upload-file",     "--proto",
};

fn isOne(word: []const u8, of: []const []const u8) bool {
    for (of) |o| if (std.mem.eql(u8, word, o)) return true;
    return false;
}

/// Shell words: single quotes literal, double quotes with `\` escapes, a
/// bare `\` escaping the next character — and a `\` before a newline is
/// the continuation a browser writes between arguments.
fn split(gpa: std.mem.Allocator, s: []const u8, out: *std.ArrayList([]const u8)) !void {
    var word: std.ArrayList(u8) = .empty;
    defer word.deinit(gpa);
    var in_word = false;
    var i: usize = 0;
    while (i < s.len) : (i += 1) {
        const c = s[i];
        switch (c) {
            '\'' => {
                in_word = true;
                i += 1;
                while (i < s.len and s[i] != '\'') : (i += 1) try word.append(gpa, s[i]);
            },
            '"' => {
                in_word = true;
                i += 1;
                while (i < s.len and s[i] != '"') : (i += 1) {
                    if (s[i] == '\\' and i + 1 < s.len and (s[i + 1] == '"' or s[i + 1] == '\\' or s[i + 1] == '$' or s[i + 1] == '`')) i += 1;
                    try word.append(gpa, s[i]);
                }
            },
            '\\' => {
                if (i + 1 >= s.len) continue;
                i += 1;
                if (s[i] == '\n') continue;
                if (s[i] == '\r' and i + 1 < s.len and s[i + 1] == '\n') {
                    i += 1;
                    continue;
                }
                in_word = true;
                try word.append(gpa, s[i]);
            },
            ' ', '\t', '\n', '\r' => if (in_word) {
                try out.append(gpa, try word.toOwnedSlice(gpa));
                in_word = false;
            },
            else => {
                in_word = true;
                try word.append(gpa, c);
            },
        }
    }
    if (in_word) try out.append(gpa, try word.toOwnedSlice(gpa));
}

test "a bare URL is a URL" {
    const a = try parse(std.testing.allocator, "  https://x.y/a.bin\n");
    defer a.free(std.testing.allocator);
    try std.testing.expectEqualStrings("https://x.y/a.bin", a.url);
    try std.testing.expectEqual(@as(usize, 0), a.headers.len);
}

test "a browser's curl line: the URL, the headers in order, cookies, and nothing else" {
    const line =
        \\curl 'https://x.y/a.bin?sig=1' \
        \\  -H 'accept: */*' \
        \\  -H 'authorization: Bearer t' \
        \\  -b 'k=v; k2=v2' \
        \\  -A "Mozilla/5.0 (X11; \"Linux\")" \
        \\  -e https://x.y/ \
        \\  -u me:pw \
        \\  -X GET --compressed -o out.bin --data-raw '{"a":1}' -L
    ;
    const a = try parse(std.testing.allocator, line);
    defer a.free(std.testing.allocator);
    try std.testing.expectEqualStrings("https://x.y/a.bin?sig=1", a.url);
    try std.testing.expectEqualStrings("out.bin", a.out.?);
    try std.testing.expectEqual(@as(usize, 6), a.headers.len);
    try std.testing.expectEqualStrings("accept: */*", a.headers[0]);
    try std.testing.expectEqualStrings("authorization: Bearer t", a.headers[1]);
    try std.testing.expectEqualStrings("Cookie: k=v; k2=v2", a.headers[2]);
    try std.testing.expectEqualStrings("User-Agent: Mozilla/5.0 (X11; \"Linux\")", a.headers[3]);
    try std.testing.expectEqualStrings("Referer: https://x.y/", a.headers[4]);
    try std.testing.expectEqualStrings("Authorization: Basic bWU6cHc=", a.headers[5]);
}

test "a curl line without a URL is refused, and `curled` is a URL" {
    try std.testing.expectError(error.NoUrl, parse(std.testing.allocator, "curl -H 'a: b'"));
    const a = try parse(std.testing.allocator, "curled");
    defer a.free(std.testing.allocator);
    try std.testing.expectEqualStrings("curled", a.url);
}
