"""In-place file edits for scripts/admin-e2e (same inode, so a container's bind mount of the
file sees the change — replacing the file would leave the container on the old inode).

Usage: edit.py replace <path> <old> <new>   (first occurrence; fails if absent)
       edit.py write <path> <content>
"""
import sys

mode, path = sys.argv[1], sys.argv[2]
if mode == "replace":
    old, new = sys.argv[3], sys.argv[4]
    text = open(path).read()
    if old not in text:
        sys.exit(f"{old!r} not found in {path}")
    text = text.replace(old, new, 1)
elif mode == "write":
    text = sys.argv[3]
else:
    sys.exit("usage: edit.py replace|write <path> ...")
with open(path, "r+") as f:
    f.seek(0)
    f.write(text)
    f.truncate()
