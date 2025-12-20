import os
import shutil
import tempfile
from pathlib import Path

from .time import utc_now_iso


def ensure_parent(path: Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def safe_unlink(path: os.PathLike[str] | str) -> None:
    try:
        Path(path).unlink(missing_ok=True)
    except Exception:
        return


def relpath_posix(path: Path, base_dir: Path) -> str:
    return path.relative_to(base_dir).as_posix()


def file_size(path: Path) -> int:
    return int(path.stat().st_size)


def fsync_dir(parent: Path) -> None:
    """
    Ensure directory entry durability after atomic rename.
    """
    fd: int | None = None
    try:
        fd = os.open(parent, os.O_RDONLY)
        os.fsync(fd)
    finally:
        if fd is not None:
            os.close(fd)


def fsync_file(path: Path) -> None:
    try:
        with path.open("rb") as f:
            os.fsync(f.fileno())
    except Exception:
        return


def atomic_write_text(
    path: Path,
    text: str,
    *,
    encoding: str = "utf-8",
    newline: str = "\n",
    mode: int = 0o644,
) -> None:
    """
    Atomically write text to `path`.

    Guarantees:
      - readers either see the old complete file or the new complete file
      - no partial/truncated file on crash
      - temp file written in the same directory (atomic replace works)
      - file contents are fsync()'d before replace
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Use a named temp file in the same directory.
    fd: int | None = None
    tmp_path: Path | None = None

    try:
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=str(path.parent),
            text=True,
        )
        tmp_path = Path(tmp_name)

        # Write, Flush, FSync process
        with os.fdopen(fd, "w", encoding=encoding, newline=newline) as f:
            fd = None
            f.write(text)
            f.flush()
            os.fsync(f.fileno())

        try:
            os.chmod(tmp_path, mode)
        except Exception:
            pass

        os.replace(tmp_path, path)

        fsync_dir(path.parent)

    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        if tmp_path is not None and tmp_path.exists():
            safe_unlink(tmp_path)


def atomic_dir_swap(final_dir: Path, tmp_dir: Path) -> None:
    """
    Swap tmp_dir into final_dir with rollback.
    """

    final_dir = Path(final_dir)
    tmp_dir = Path(tmp_dir)
    parent = final_dir.parent
    parent.mkdir(parents=True, exist_ok=True)

    stamp = utc_now_iso().replace(":", "").replace("-", "").replace(".", "")
    backup_dir = parent / f"{final_dir.name}.old.{stamp}"

    if final_dir.exists():
        if backup_dir.exists():
            # best-effort cleanup
            try:
                for p in sorted(backup_dir.rglob("*"), reverse=True):
                    if p.is_file():
                        p.unlink(missing_ok=True)
                    else:
                        p.rmdir()
                backup_dir.rmdir()
            except Exception:
                pass
        final_dir.rename(backup_dir)

    try:
        tmp_dir.rename(final_dir)
    except Exception:
        # rollback if possible
        try:
            if backup_dir.exists() and not final_dir.exists():
                backup_dir.rename(final_dir)
        except Exception:
            pass
        raise
    finally:
        # cleanup backup
        if backup_dir.exists():
            try:
                for p in sorted(backup_dir.rglob("*"), reverse=True):
                    if p.is_file():
                        p.unlink(missing_ok=True)
                    else:
                        p.rmdir()
                backup_dir.rmdir()
            except Exception:
                pass


def atomic_replace(tmp_path: Path, final_path: Path) -> None:
    with open(tmp_path, "rb") as f:
        os.fsync(f.fileno())

    os.replace(tmp_path, final_path)

    dir_fd = os.open(str(final_path.parent), os.O_DIRECTORY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def make_tmp_dir_for(final_dir: Path) -> Path:
    """
    Create a temp dir next to final_dir (same filesystem) so rename is atomic.
    """
    final_dir = Path(final_dir)
    final_dir.parent.mkdir(parents=True, exist_ok=True)
    tmp = tempfile.mkdtemp(prefix=f".{final_dir.name}.tmp.", dir=str(final_dir.parent))
    return Path(tmp)


def copy_or_hardlink(src: Path, dst: Path) -> None:
    """
    Prefer hardlink (O(1), no extra disk), fallback to copy2.
    """
    src = Path(src)
    dst = Path(dst)
    ensure_parent(dst)
    try:
        os.link(src, dst)
    except Exception:
        shutil.copy2(src, dst)


def atomic_write_bytes(path: Path, data: bytes, *, mode: int = 0o644) -> None:
    """
    Atomically write bytes to `path` with fsync + dir fsync.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fd: int | None = None
    tmp_path: Path | None = None
    try:
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=str(path.parent),
            text=False,
        )
        tmp_path = Path(tmp_name)

        with os.fdopen(fd, "wb") as f:
            fd = None
            f.write(data)
            f.flush()
            os.fsync(f.fileno())

        try:
            os.chmod(tmp_path, mode)
        except Exception:
            pass

        os.replace(tmp_path, path)
        fsync_dir(path.parent)
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        if tmp_path is not None and tmp_path.exists():
            safe_unlink(tmp_path)


def atomic_dir_commit(
    *, tmp_dir: Path, final_dir: Path, overwrite: bool = False
) -> None:
    """
    Convenience wrapper for publish/package stages.
    """
    final_dir = Path(final_dir)
    tmp_dir = Path(tmp_dir)

    if final_dir.exists() and not overwrite:
        raise FileExistsError(f"Target exists (overwrite disabled): {final_dir}")

    atomic_dir_swap(final_dir, tmp_dir)
