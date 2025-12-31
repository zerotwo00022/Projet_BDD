from abc import ABC, abstractmethod

class IRecordIterator(ABC):
    @abstractmethod
    def GetNextRecord(self):
        """Retourne le prochain Record ou None si fini."""
        pass

    @abstractmethod
    def Close(self):
        """Libère les ressources (buffers, fichiers)."""
        pass

    @abstractmethod
    def Reset(self):
        """Repart du début."""
        pass