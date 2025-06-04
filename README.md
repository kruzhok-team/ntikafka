ntikafka
========

Go библиотека для взаимодействия с Kafka используемым на платформе Талант.

На данный момент включает логику запуска консьюмера и декодеры сообщений записанных Debezium.

Существующие декодеры не аллоцируют память для декодирования, только для телеметрии.
При разработке новых и изменении существующих декодеров,
нужно избегать аллокаций насколько это возможно.


Установка
---------

1. Устанавливается в проекты как git submodule:

```sh
git submodule add ../go/ntikafka vendors/ntikafka
```

Обратите внимание что имя директории `vendors`, а не `vendor`, для которой в go специальное значение.

2. В `go.mod` нужно добавить строку замены:

```go.mod
replace gitlab.jetstyle.in/jetstyle/nti/go/ntikafka => ./vendors/ntikafka/
```

После этого можно добавить в проект (go get) и импортировать в коде пакет `gitlab.jetstyle.in/jetstyle/nti/go/ntikafka`.

3. Для использования в CI [нужно добавить переменную](https://docs.gitlab.com/ci/runners/git_submodules/) в `.gitlab-ci.yaml`:

```yaml
variables:
  GIT_SUBMODULE_STRATEGY: recursive
```

4. В `Dockerfile`, перед выполнением `go mod download` нужно скопировать в образ директорию `vendors`.


Зависимые проекты
-----------------

- [talent-v2](https://gitlab.jetstyle.in/jetstyle/nti/talent-v2)
- [berloga-awards](https://gitlab.jetstyle.in/jetstyle/nti/berloga-awards)
- [calcon](https://gitlab.jetstyle.in/jetstyle/nti/calc-constructor-back)
- [drugoe-delo](https://gitlab.jetstyle.in/jetstyle/nti/drugoe-delo)
