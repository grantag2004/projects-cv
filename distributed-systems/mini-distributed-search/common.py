# common.py
import os
import re

AMQP_URL = os.environ.get("AMQP_URL", "amqp://guest:guest@127.0.0.1:5672/%2F")

MANAGER_RPC_QUEUE = "manager.rpc"

WORKER_QUEUE_FMT = "worker.{id}.rpc"

MANAGER_CTRL_QUEUE_FMT = "manager.{id}.ctrl"          
MANAGER_BROADCAST_EXCHANGE = "managers.broadcast"      

HEARTBEAT_INTERVAL = float(os.environ.get("HB_INTERVAL", "1.0")) 
HEARTBEAT_TIMEOUT  = float(os.environ.get("HB_TIMEOUT",  "3.0"))  
ELECTION_REPLY_TIMEOUT = float(os.environ.get("ELECT_T_REPLY","1.5")) 
COORDINATOR_WAIT_TIMEOUT = float(os.environ.get("ELECT_T_COORD","2.0"))

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
NODES_DIR = os.path.join(PROJECT_ROOT, "nodes")

# Список стоп-слов (русские и английские)
STOP_WORDS = {
    'и', 'в', 'во', 'не', 'что', 'он', 'на', 'я', 'с', 'со', 'как', 'а', 'то', 'все',
    'она', 'так', 'его', 'но', 'да', 'ты', 'к', 'у', 'же', 'вы', 'за', 'бы', 'по',
    'только', 'ее', 'мне', 'было', 'вот', 'от', 'меня', 'еще', 'нет', 'о', 'из',
    'ему', 'теперь', 'когда', 'даже', 'ну', 'вдруг', 'ли', 'если', 'уже', 'или',
    'ни', 'быть', 'был', 'него', 'до', 'вас', 'нибудь', 'опять', 'уж', 'вам', 'ведь',
    'там', 'потом', 'себя', 'ничего', 'ей', 'может', 'они', 'тут', 'где', 'есть',
    'надо', 'ней', 'для', 'мы', 'тебя', 'их', 'чем', 'была', 'сам', 'чтоб', 'без',
    'будто', 'чего', 'раз', 'тоже', 'себе', 'под', 'будет', 'ж', 'тогда', 'кто',
    'этот', 'того', 'потому', 'этого', 'какой', 'совсем', 'ним', 'здесь', 'этом',
    'один', 'почти', 'мой', 'тем', 'чтобы', 'нее', 'сейчас', 'были', 'куда', 'зачем',
    'всех', 'никогда', 'можно', 'при', 'наконец', 'два', 'об', 'другой', 'хоть',
    'после', 'над', 'больше', 'тот', 'через', 'эти', 'нас', 'про', 'всего', 'них',
    'какая', 'много', 'разве', 'три', 'эту', 'моя', 'впрочем', 'хорошо', 'свою',
    'этой', 'перед', 'иногда', 'лучше', 'чуть', 'том', 'нельзя', 'такой', 'им',
    'более', 'всегда', 'конечно', 'всю', 'между',
    
    # Английские стоп-слова
    'a', 'an', 'the', 'and', 'but', 'or', 'if', 'because', 'as', 'until', 'while',
    'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through',
    'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in',
    'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here',
    'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few',
    'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own',
    'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don',
    'should', 'now'
}

def node_dir(node_id: int) -> str:
    d = os.path.join(NODES_DIR, f"node{int(node_id)}")
    os.makedirs(d, exist_ok=True)
    return d

_SENT_SPLIT_RE = re.compile(r'[.!?]+\s+')
_SPACES_RE = re.compile(r'\s+')
_WORD_RE = re.compile(r'\b\w+\b', re.UNICODE | re.IGNORECASE)

def preprocess_line(line: str) -> str:
    line = (line or "").strip()
    line = _SPACES_RE.sub(" ", line)
    return line

def split_sentences(text: str):
    text = text or ""
    parts = _SENT_SPLIT_RE.split(text)
    out = []
    for p in parts:
        p = p.strip()
        if p:
            out.append(p)
    return out

def contains_word(sentence: str, word: str) -> bool:
    pattern = re.compile(rf"\b{re.escape(word)}\b", re.IGNORECASE)
    return bool(pattern.search(sentence or ""))

def extract_words(text: str, remove_stopwords: bool = True):
    """Извлечь слова из текста, опционально удаляя стоп-слова"""
    words = [w.lower() for w in _WORD_RE.findall(text)]
    if remove_stopwords:
        words = [w for w in words if w not in STOP_WORDS]
    return words

def calculate_cosine_similarity(vec1: dict, vec2: dict) -> float:
    """Вычислить косинусную схожесть между двумя векторами"""
    # Найти общие слова
    common_words = set(vec1.keys()) & set(vec2.keys())
    
    # Вычислить скалярное произведение
    dot_product = sum(vec1[word] * vec2[word] for word in common_words)
    
    # Вычислить нормы векторов
    norm1 = sum(v ** 2 for v in vec1.values()) ** 0.5
    norm2 = sum(v ** 2 for v in vec2.values()) ** 0.5
    
    if norm1 == 0 or norm2 == 0:
        return 0.0
    
    return dot_product / (norm1 * norm2)