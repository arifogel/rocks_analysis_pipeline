FROM almalinux:9
RUN dnf install -y --setopt=keepcache=True \
            gcc-c++ \
            git \
            wget \
            tar \
            cpio
# SSH setup
RUN mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    ssh-keyscan gitlab.com >> /root/.ssh/known_hosts
#RUN --mount=type=ssh git clone git@github.com:myorg/private-lib.git /app/lib

# Install bazelisk
RUN wget -q -O /usr/local/bin/bazel https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-linux-amd64 \
&&  chmod +x /usr/local/bin/bazel
RUN echo 'build --disk_cache=/root/.cache/bazel/disk_cache' > /root/.bazelrc
RUN echo 'build --keep_going' >> /root/.bazelrc

RUN mkdir /work
WORKDIR /work
RUN git clone https://github.com/arifogel/rocks_analysis_pipeline
WORKDIR /work/rocks_analysis_pipeline

#RUN echo 'common --override_module=ghcss=/work/ghcss' > /work/rocks_analysis_pipeline/.bazelrc.user
#RUN  --mount=type=ssh git clone git@github.com:arifogel/ghcss /work/ghcss

