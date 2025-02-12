def do_tar(
    src_path_prefix: str,
    src_path: str = "",
    dest_path: str = "",
) -> list[str]:
    import tarfile
    from pathlib import Path

    args = []
    dest_path_prefix = "/"

    """
    For each source directory *src_path* tar the contents of the directory
    and write the compressed output to *dest_path*

    :param src_path_prefix:    The source path prefix to replace
    :param dest_path_prefix:   Path prefix to substitute the src prefix with
    :param src_path:           The source file or directory
    :param dest_path:          The destination path to tar/gzip to.
                                 If this is a directory, a file name will be
                                 auto generated based on the source name
    :optional src/dest pairs:  Additional src_path/dest_path pairs to add

    This function takes a variable number of args, additional ones in pairs,
    and generates potentially multiple output .tar.gz files.  Multiple input
    paths can be added to the same *dest_path*.

    The prefix arguments will be used to remap all src_path arguments when
    the mapped collection actual file system paths need to be transformed

    :returns:  A list of absolute output file paths of the .tar.gz files

    :raises: ValueError if the source paths are not accessible or if the
               destination paths are missing for a source path
    """
    if not src_path or not dest_path:
        raise ValueError("Both source directory and destination path must be provided")
    elif len(args) % 2 == 1:
        raise ValueError("Additional src_dir and dest_path must be given in pairs")

    if bool(src_path_prefix) != bool(dest_path_prefix):
        raise ValueError(
            "Prefix paths must be given for both source and destination or neither"
        )
    elif src_path_prefix and dest_path_prefix:
        src_path_prefix = src_path_prefix.strip()
        dest_path_prefix = dest_path_prefix.strip()
        src_slash = src_path_prefix.endswith("/")
        dest_slash = dest_path_prefix.endswith("/")
        if src_slash and not dest_slash:
            dest_path_prefix += "/"
        elif not src_slash and dest_slash:
            src_path_prefix += "/"

    inputs = [src_path, dest_path]
    inputs.extend([arg for arg in args])
    transformed_paths = []
    for p in inputs:
        if p.startswith(src_path_prefix):
            transformed_paths.append(p.replace(src_path_prefix, dest_path_prefix, 1))
        else:
            raise ValueError(f"Source path {p} does not begin with {src_path_prefix}")

    # Map unique destination paths to a set of source paths
    tar_map = {}
    for i in range(0, len(transformed_paths), 2):
        src_dir = Path(transformed_paths[i])
        if not src_dir.exists():
            raise ValueError(f"Source path {src_dir.absolute()} could not be located")
        cur_dest = transformed_paths[i + 1]
        if cur_dest not in tar_map:
            tar_map[cur_dest] = []

        tar_map[cur_dest].append(src_dir)

    result_paths = []
    for dest_path, src_dirs in tar_map.items():
        dest = Path(dest_path)
        if dest.is_dir():
            # A directory destination specified means generating a filename
            # from the source path
            dest_zip_path = dest / (src_dirs[0].name + ".tar.gz")
        else:
            # Reformat the destination filename if not already standardized
            fn = dest.name
            if fn.endswith(".gz"):
                # If ending in .gz we assume that's the gzip final path they want
                # even if not .tar.gz
                pass
            elif fn.endswith(".tar"):
                fn = fn + ".gz"
            else:
                fn = fn + ".tar.gz"
            dest_zip_path = dest.with_name(fn)

        print(f"About to create tar file {dest_zip_path.absolute()} from {src_dirs}")
        tar = tarfile.open(dest_zip_path, "w:gz")
        for src_dir in src_dirs:
            tar.add(src_dir, arcname=src_dir.name)
        tar.close()
        result_paths.append(str(dest_zip_path.absolute()))
    return result_paths


"""
 d0b97d76-fd8a-4139-8e33-08720bff1a63
 compute=> select id,user_id,public,function_name from functions where function_uuid='36282764-9b17-4367-8da7-ba1468f9258f'; # noqa E501
    id    | user_id | public | function_name
 ---------+---------+--------+---------------
  1295916 |     389 | t      | do_tar
"""


if __name__ == "__main__":
    from globus_compute_sdk import Client

    gcc = Client()
    fuid = gcc.register_function(do_tar)
    print(f"Tar func UUID is {fuid}")
    # do_tar(*sys.argv[1:])
