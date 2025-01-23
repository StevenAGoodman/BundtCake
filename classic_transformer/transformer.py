import numpy as np
import torch
import torch.nn as nn

class Embedding(nn.Module):
    def __init__(self, d_model: int, vocab_size: int):
        super().__init__()
        self.d_model = d_model
        self.vocab_size = vocab_size
        self.embedding = nn.Embedding(vocab_size, d_model)

    def forward(self, x):
        return self.embedding(x) * np.sqrt(self.d_model)

class PosEncoding(nn.Module):
    def __init__(self, d_model: int, seq: int, dropout: float) -> None:
        super().__init__()
        self.d_model = d_model
        self.seq = seq
        self.dropout = nn.Dropout(dropout)

        pe = torch.zeros(seq, d_model)
        position = torch.arange(0, seq, dtype=torch.float).unsqueeze(1)
        div_term = torch.exp(torch.arange(0, d_model, 2).float() * (-np.log(10000.0) / d_model))

        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        pe = pe.unsqueeze(0)
        self.register_buffer("pe", pe)

    def forward(self, x):
        x = x + (self.pe[:, :x.shape[1], :]).requires_grad_(False)
        return self.dropout(x)

class LayerNorm(nn.Module):
    """
    Normalizing a batch of multiple seqs
        - fancy equation relating mean, std and learnable params to normalized vectors
    """
    def __init__(self, features: int, eps: float=10**-6) -> None:
        super().__init__()
        self.eps = eps
        self.alpha = nn.Parameter(torch.ones(features))
        self.bias = nn.Parameter(torch.zeros(features))

    def forward(self, x):
        mean = x.mean(dim=-1, keepdim=True)
        std = x.std(dim=-1, keepdim=True)
        return self.alpha * (x - mean) / (std + self.eps) + self.bias

class FeedForwardBlock(nn.Module):
    """
    Big neural net in the middle to learn complex, nonlinear trends in the abstract embeddings. Kinda the core where encoder and decoder meet?
        - fully connected layer to (usually) higher dim d_ff
        - relu for compression
        - fully connected layer back to original dims
    """
    def __init__(self, d_model: int, d_ff: int, dropout: float) -> None:
        super().__init__()
        self.linear_1 = nn.Linear(d_model, d_ff)
        self.dropout = nn.Dropout(dropout)
        self.linear_2 = nn.Linear(d_ff, d_model)
    
    def forward(self, x):
        return self.linear_2(self.dropout(torch.relu(self.linear_1(x))))

class ResidualConn(nn.Module):
    """
    General idea is to have a copy of the orginal information skip over a layer to while the og goes through and adding at the end. 
    Prevents them from "forgetting"
        - intakes x and desired layer sublayer
        - runs through normalizing
        - apply sublayer
        -
    """
    def __init__(self, features: int, dropout: float) -> None:
        super().__init__()
        self.dropout = nn.Dropout(dropout)
        self.norm = LayerNorm(features)

    def forward(self, x, sublayer):
        return x + self.dropout(sublayer(self.norm(x)))

class MultiHeadAttentionBlock(nn.Module):
    """
    Better self attention where tokens are split into h heads allowing for deeper weights to be learned
        - intake query, key, and value matrices 
        - multiply with corresponding learned weight matrices (w_q, w_k, w_v)
        - split into h heads along the d_model dim (not seq)
        - do fancy math equation to combine mini q, k, v matrices individually
        - concat the individual head values
        - multiply by last weight matrix w_o 
    """
    def __init__(self, d_model: int, h: int, dropout: float) -> None:
        super().__init__()
        self.d_model = d_model
        self.h = h
        assert d_model % h == 0, "d_model not divisable by h"

        self.d_k = d_model // h
        # a bunch of linear models that represent weight matrices, i think?
        self.w_q = nn.Linear(d_model, d_model, bias=False)
        self.w_k = nn.Linear(d_model, d_model, bias=False)
        self.w_v = nn.Linear(d_model, d_model, bias=False)
        self.w_o = nn.Linear(d_model, d_model, bias=False)
        self.dropout = nn.Dropout(dropout)

    @staticmethod
    def attention(query, key, value, mask, dropout: nn.Dropout):
        d_k = query.shape[-1]

        attention_scores = (query @ key.transpose(-2, -1)) / np.sqrt(d_k)
        if mask is not None: attention_scores.masked_fill_(mask == 0, -1e9)
        attention_scores = attention_scores.softmax(dim=-1)
        if dropout is not None: attention_scores = dropout(attention_scores)

        return (attention_scores @ value), attention_scores

    def forward(self, q, k, v, mask):
        # multiply weights through fully connected layer thing
        query = self.w_q(q)
        key = self.w_k(k)
        value = self.w_v(v)

        # change dimensions and dim order for matrices
        query = query.view(query.shape[0], query.shape[1], self.h, self.d_k).transpose(1,2)
        key = key.view(key.shape[0], key.shape[1], self.h, self.d_k).transpose(1,2)
        value = value.view(value.shape[0], value.shape[1], self.h, self.d_k).transpose(1,2)

        # ACTUAL ATTENTION CALCULATION 
        x, self.attention_scores = MultiHeadAttentionBlock.attention(query, key, value, mask, self.dropout)
        
        # return to og shape and concat
        x = x.transpose(1,2).contiguous().view(x.shape[0], -1, self.h * self.d_k)
        return self.w_o(x)



