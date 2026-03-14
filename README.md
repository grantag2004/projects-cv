# Academic ML & Distributed Systems Portfolio

Учебный репозиторий-портфолио с работами по deep learning, продуктовой аналитике, машинному обучению, математической статистике и распределённым системам.

## Что внутри

- **Deep Learning** — ноутбуки по PyTorch, семантической сегментации и NER.
- **Product Analytics** — задания по теории вероятностей, математической статистике и основам машинного обучения.
- **Distributed Systems** — мини-распределённая система поиска по текстам с репликацией, leader election и TF-IDF-поиском.
- **Research** — курсовая работа по распределённому обучению CNN на GPU.

## Структура репозитория

```text
.
├── deep-learning/
│   ├── README.md
│   └── notebooks/
│       ├── mlp_mnist_pytorch.ipynb
│       ├── multiclass_semantic_segmentation.ipynb
│       └── named_entity_recognition_transformers.ipynb
├── distributed-systems/
│   ├── README.md
│   └── mini-distributed-search/
│       ├── README.md
│       ├── client.py
│       ├── common.py
│       ├── manager.py
│       ├── requirements.txt
│       └── worker.py
├── product-analytics/
│   ├── README.md
│   ├── machine-learning-basics/
│   │   ├── feature_importance_credit_default.ipynb
│   │   ├── knn_multiclass_classification.ipynb
│   │   ├── linear_models_and_regularization.ipynb
│   │   ├── classification_pipeline_model_comparison.ipynb
│   │   ├── trees_ensembles_and_boosting.ipynb
│   │   └── preprocessing_impact_on_model_quality.ipynb
│   └── probability-and-statistics/
│       ├── power_analysis_and_ab_statistics.ipynb
│       ├── probability_distributions_visualization.ipynb
│       └── statistical_tests_and_t_distribution.ipynb
├── research/
│   ├── README.md
│   └── distributed-cnn-training/
│       ├── README.md
│       └── report/
│           └── distributed_cnn_training_coursework.pdf
├── docs/
│   ├── github_about.md
│   ├── publish_checklist.md
│   └── rename_map.md
└── .gitignore
```

## Избранные проекты

### 1) Mini Distributed Search System
Мини-распределённая система поиска по текстовым документам на Python + RabbitMQ.

Что реализовано:
- клиентская консоль;
- manager/worker архитектура;
- резервные менеджеры и election;
- primary/replica worker-модель;
- поиск по слову;
- поиск похожих документов через TF-IDF.

См. `distributed-systems/mini-distributed-search/README.md`.

### 2) Distributed CNN Training Coursework
Курсовая работа по исследованию распределённого обучения CNN на GPU:
- DDP,
- pipeline parallelism,
- tensor parallelism,
- сравнение на ResNet-152 / Tiny-ImageNet-200.

См. `research/distributed-cnn-training/README.md`.

### 3) Deep Learning Notebooks
Подборка учебных ноутбуков по deep learning:
- полносвязные сети на PyTorch,
- семантическая сегментация,
- NER.

### 4) Product Analytics & ML
Ноутбуки по вероятности, статистике, A/B-мышлению и основам ML:
- визуализация распределений,
- t-test и power analysis,
- KNN,
- линейные модели,
- деревья и ансамбли,
- feature importance.

## Почему репозиторий выглядит аккуратно

- файлы переименованы в понятный единый стиль;
- курсовые и домашние работы разнесены по смысловым разделам;
- конфликтующие названия вроде `Task2.ipynb` и `task2.ipynb` разведены в разные осмысленные имена;
- для каждого крупного раздела добавлены README-файлы;
- внутри `docs/` собраны материалы для оформления GitHub-страницы.

## Что ещё можно сделать после публикации

1. Добавить скриншоты ключевых ноутбуков и графиков в отдельную папку `assets/`.
2. Вынести лучшие проекты в отдельные самостоятельные репозитории.
3. Добавить English version README, если репозиторий нужен для более широкой аудитории.
4. Проставить GitHub topics и короткое описание в разделе **About**.

## Примечание

Большая часть материалов носит учебный характер. Поэтому репозиторий лучше позиционировать как **academic / educational portfolio**.
