'''
Модуль CLI-профилирования времени выполнения кода.

Инструмент для изолированного замера отдельных блоков и формирования итоговых отчетов в стандартном табличном CLI-формате.
'''
import time
from typing import List, Tuple, Type, Optional

from config import _conf

class Timer:
    '''
    Контекстный менеджер для точного измерения времени выполнения.
    
    Накапливает результаты замеров в статическом хранилище для
    последующей генерации сводных таблиц в стиле cProfile.
    
    :cvar list records: Статическое хранилище записей формата (имя_блока, время_в_секундах).
    '''

    records: List[Tuple[str, float]] = []

    def __init__(self, block_name: str = 'Block') -> None:
        '''
        Инициализирует таймер для конкретного блока кода.
        
        :param str block_name: Идентификатор замеряемого участка кода для вывода в CLI.
        '''
        self.block_name: str = block_name
        self.start: float = 0.0

    def __enter__(self) -> 'Timer':
        '''
        Фиксирует временную метку начала выполнения блока кода.
        
        :return: Экземпляр текущего контекстного менеджера Timer.
        :rtype: Timer
        '''
        self.start = time.perf_counter()
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]], 
                 exc_val: Optional[BaseException], 
                 exc_tb: Optional[object]) -> None:
        '''
        Вычисляет чистую длительность блока и сохраняет её в историю.
        
        Гарантирует корректную запись метрик даже при возникновении
        исключений внутри контекста.
        
        :param Optional[Type[BaseException]] exc_type: Тип возникшего исключения.
        :param Optional[BaseException] exc_val: Экземпляр исключения.
        :param Optional[object] exc_tb: Объект трассировки стека (traceback).
        '''
        elapsed = time.perf_counter() - self.start
        Timer.records.append((self.block_name, elapsed))

    @classmethod
    def report(cls) -> None:
        '''
        Выводит структурированный CLI-отчет по всем накопленным замерам.
        
        Рассчитывает абсолютное время в миллисекундах и относительную долю 
        каждого блока от общего времени выполнения. Вывод выравнивается 
        по правому и левому краям для сохранения табличной структуры.
        '''
        if not cls.records or not _conf.get('profiler.timer.report', False):
            return
            
        total_time = sum(t for _, t in cls.records)
        
        print(f"\n{'Block Name':<25} | {'Time (ms)':>12} | {'Time %':>8}")
        print("-" * 52)
        
        for name, duration in cls.records:
            ms = duration * 1000
            pct = (duration / total_time) * 100 if total_time > 0 else 0
            print(f"{name:<25} | {ms:>12.3f} | {pct:>7.1f}%")
            
        print("-" * 52)
        print(f"{'TOTAL':<25} | {total_time * 1000:>12.3f} | 100.0%\n")