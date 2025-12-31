class PageId:
    def __init__(self, file_idx: int, page_idx: int):
        self.FileIdx = file_idx  # L'index du fichier (le X dans DataX.bin)
        self.PageIdx = page_idx  # L'index de la page dans ce fichier (0, 1, 2...)

    def __eq__(self, other):
        if not isinstance(other, PageId):
            return False
        return self.FileIdx == other.FileIdx and self.PageIdx == other.PageIdx

    def __repr__(self):
        return f"PageId({self.FileIdx}, {self.PageIdx})"

    def __hash__(self):
        # Nécessaire pour utiliser PageId comme clé dans un dictionnaire (pour le BufferManager)
        return hash((self.FileIdx, self.PageIdx))