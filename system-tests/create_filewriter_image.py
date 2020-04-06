import os.path
import os
import pytest
import docker
import tarfile
import hashlib
import io

IMAGE_NAME = "screamingudder/ubuntu18.04-build-node:3.0.6"
CONTAINER_NAME = "filewriter-system-test"
DEBUG_MODE = True
client = docker.from_env()


def execute_command(command, workdir, container):
    exec_id = container.client.api.exec_create(container.id, command, workdir=workdir)[
        "Id"
    ]
    output = container.client.api.exec_start(exec_id, stream=True)
    for line in output:
        if DEBUG_MODE:
            print(line.decode("utf-8"))
    exit_code = client.api.exec_inspect(exec_id)["ExitCode"]
    if exit_code != 0:
        raise RuntimeError(
            "The following command failed with a non-zero exit code ({}): {}".format(
                exit_code, command
            )
        )


def copy_files_to_container(paths, base_path, container):
    in_memory_file = io.BytesIO()
    tar = tarfile.open(fileobj=in_memory_file, mode="w")
    for path in paths:
        tar.add(arcname=path, name=base_path + path)
    tar.close()
    container.put_archive("/home/jenkins/", in_memory_file.getvalue())
    return hashlib.sha512(in_memory_file.getvalue()).hexdigest()


def create_hash_file(file_name, hash):
    out_file = open(file_name, "w")
    out_file.write(hash)
    out_file.close()


def copy_to_container(container):
    conanfile = "conan/conanfile.txt"
    conan_hash = copy_files_to_container([conanfile,], "../", container)
    source_files = ["cmake/", "src/", "CMakeLists.txt", "docker_launch.sh"]
    src_hash = copy_files_to_container(source_files, "../", container)

    src_hash_file_name = "src_hash_new.txt"
    conan_hash_file_name = "conan_hash_new.txt"
    create_hash_file(src_hash_file_name, src_hash)
    create_hash_file(conan_hash_file_name, conan_hash)

    copy_files_to_container([src_hash_file_name, conan_hash_file_name], "./", container)


def generate_new_container():
    environment_variables = {}

    def add_var(var_name):
        if var_name in os.environ:
            environment_variables[var_name] = os.environ[var_name]

    add_var("http_proxy")
    add_var("https_proxy")
    add_var("local_conan_server")
    try:
        client.images.get(IMAGE_NAME)
    except docker.errors.ImageNotFound as e:
        client.images.pull(IMAGE_NAME)
    container = client.containers.create(
        IMAGE_NAME,
        name=CONTAINER_NAME,
        command="tail -f /dev/null",
        environment=environment_variables,
    )
    container.start()
    container.exec_run("apt-get --assume-yes install kafkacat", user="root")
    if "local_conan_server" in os.environ:
        print("Setting up local conan server")
        container.exec_run(
            "conan remote add --insert 0 ess-dmsc-local {}".format(
                os.environ["local_conan_server"]
            )
        )
    else:
        print("No local conan server available in environment variables.")
    return container


def kill_and_remove(container):
    try:
        container.kill()
        container.remove()
    except docker.errors.APIError as e:
        pass


def run_conan(container):
    print("Running conan")
    container.exec_run("mkdir build", workdir="/home/jenkins/")
    execute_command(
        "conan install --build=outdated ../conan", "/home/jenkins/build", container
    )
    execute_command("cp conan_hash_new.txt conan_hash.txt", "/home/jenkins/", container)
    print("Done running conan")


def rebuild_filewriter(container):
    print("Re-building the filewriter")
    execute_command(
        'bash -c "bash activate_run.sh && cmake .. -GNinja -DCONAN=MANUAL-DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=False -DRUN_DOXYGEN=False && ninja kafka-to-nexus"',
        "/home/jenkins/build",
        container,
    )
    execute_command("cp src_hash_new.txt src_hash.txt", "/home/jenkins/", container)
    print("Done building the filewriter")


def re_generate_test_image(container):
    print("Generating docker image")
    container.commit("filewriter-image", changes='CMD ["./docker_launch.sh"]')


def conan_hash_changed(container):
    return (
        container.exec_run("less conan_hash_new.txt").output
        != container.exec_run("less conan_hash.txt").output
    )


def src_hash_changed(container):
    return (
        container.exec_run("less src_hash_new.txt").output
        != container.exec_run("less src_hash.txt").output
    )


def create_filewriter_image():
    list_of_containers = client.containers.list("all")
    found_container = False
    for c_container in list_of_containers:
        if c_container.attrs["Name"][1:] == CONTAINER_NAME:
            print("Found existing filewriter container")
            found_container = True
            container = client.containers.get(CONTAINER_NAME)
            if container.attrs["Config"]["Image"] != IMAGE_NAME:
                print("Filewriter container uses outdated image, ignoring")
                kill_and_remove(container)
                found_container = False
            break
    if not found_container:
        print("Unable to find container, re-generating")
        container = generate_new_container()
    copy_to_container(container)
    try:
        container.start()
    except docker.errors.APIError as e:
        pass

    if conan_hash_changed(container):
        print("Conan package list outdated")
        run_conan(container)
        rebuild_filewriter(container)
        re_generate_test_image(container)
    elif src_hash_changed(container):
        print("Filewriter source code has changed")
        rebuild_filewriter(container)
        re_generate_test_image(container)

    container.kill()
    print("Done creating filewriter docker image")


if __name__ == "__main__":
    create_filewriter_image()
