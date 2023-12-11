from __future__ import annotations


class TemporaryNoOpSummarizer:
    def render(self, input):
        return input


class DropAllVowelsSummarizer:
    def render(self, input):
        return input
