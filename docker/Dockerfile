# Single Ray runtime image for training and serving (CPU only).
# RAY_IMAGE default mirrors infra/versions.env; `make image` always passes it explicitly.
# Built by `make image`, then side-loaded into kind; pods never pull at start.
ARG RAY_IMAGE=rayproject/ray:2.59.0-py312-cpu
FROM ${RAY_IMAGE}

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
        --extra-index-url https://download.pytorch.org/whl/cpu \
        -r /tmp/requirements.txt \
    && python -c "import torch, transformers, datasets; print(torch.__version__, transformers.__version__, datasets.__version__)"
