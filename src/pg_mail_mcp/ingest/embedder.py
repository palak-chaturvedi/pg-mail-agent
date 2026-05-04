"""Pluggable embedder. Default: local sentence-transformers.

The embedder is loaded lazily so the MCP server can start without a model
installed (e.g. when running purely against FTS). It returns float32 vectors
serialized as bytes for sqlite-vec.
"""
from __future__ import annotations

import struct
from typing import Iterable, Optional, Protocol


class Embedder(Protocol):
    dim: int
    def embed(self, texts: list[str]) -> list[bytes]: ...


def floats_to_blob(vec: Iterable[float]) -> bytes:
    arr = list(vec)
    return struct.pack(f"<{len(arr)}f", *arr)


class _NullEmbedder:
    dim = 0
    def embed(self, texts: list[str]) -> list[bytes]:
        return [b"" for _ in texts]


class _LocalEmbedder:
    def __init__(self, model_name: str):
        from sentence_transformers import SentenceTransformer  # heavy import
        self._model = SentenceTransformer(model_name)
        self.dim = int(self._model.get_sentence_embedding_dimension())

    def embed(self, texts: list[str]) -> list[bytes]:
        if not texts:
            return []
        vecs = self._model.encode(
            texts, normalize_embeddings=True, show_progress_bar=False
        )
        return [floats_to_blob(v.tolist()) for v in vecs]


class _OpenAIEmbedder:
    def __init__(self, model_name: str, dim: int):
        from openai import OpenAI  # heavy import
        self._client = OpenAI()
        self._model = model_name
        self.dim = dim

    def embed(self, texts: list[str]) -> list[bytes]:
        if not texts:
            return []
        resp = self._client.embeddings.create(model=self._model, input=texts)
        return [floats_to_blob(d.embedding) for d in resp.data]


def make_embedder(kind: str, model: str, dim: int) -> Embedder:
    kind = (kind or "local").lower()
    if kind == "none":
        return _NullEmbedder()
    if kind == "openai":
        return _OpenAIEmbedder(model, dim)
    return _LocalEmbedder(model)


def chunk_text(text: str, max_chars: int = 1500) -> list[str]:
    """Naive char-based chunking suitable for short list-mail bodies. We chunk
    on paragraph boundaries when possible, falling back to hard splits."""
    if not text:
        return []
    text = text.strip()
    if len(text) <= max_chars:
        return [text]
    chunks: list[str] = []
    buf: list[str] = []
    size = 0
    for para in text.split("\n\n"):
        p = para.strip()
        if not p:
            continue
        if size + len(p) + 2 > max_chars and buf:
            chunks.append("\n\n".join(buf))
            buf, size = [], 0
        if len(p) > max_chars:
            # Hard split very long paragraphs.
            for i in range(0, len(p), max_chars):
                chunks.append(p[i : i + max_chars])
            continue
        buf.append(p)
        size += len(p) + 2
    if buf:
        chunks.append("\n\n".join(buf))
    return chunks
