import uuid
from django.db import models
from django.utils.translation import gettext_lazy as _
from django.core.validators import MinValueValidator, MaxValueValidator

# Create your models here.


class TimeStampedMixin(models.Model):
    # auto_now_add автоматически выставит дату создания записи
    created = models.DateTimeField(auto_now_add=True)
    # auto_now изменятся при каждом обновлении записи
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    # Типичная модель в Django использует число в качестве id. В таких
    # ситуациях поле не описывается в модели.
    # Вам же придётся явно объявить primary key.
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    # Первым аргументом обычно идёт человекочитаемое название поля
    name = models.CharField(_("name"), max_length=255)
    # blank=True делает поле необязательным для заполнения.
    description = models.TextField(_("description"), blank=True)

    def __str__(self) -> str:
        return self.name

    class Meta:
        # Ваши таблицы находятся в нестандартной схеме. Это нужно указать в
        # классе модели
        db_table = 'content"."genre'
        # Следующие два поля отвечают за название модели в интерфейсе
        verbose_name = "Жанр"
        verbose_name_plural = "Жанры"


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_("full_name"), max_length=255)

    def __str__(self) -> str:
        return self.full_name

    class Meta:
        db_table = 'content"."person'
        verbose_name = "Человек"
        verbose_name_plural = "Люди"


class Filmwork(UUIDMixin, TimeStampedMixin):
    class Types(models.TextChoices):
        MOVIE = "movie", _("movie")
        TV_SHOW = "tv_show", _("tv_show")

    title = models.CharField(_("title"), max_length=255)
    description = models.TextField(_("description"), blank=True)
    creation_date = models.DateField(_("creation_date"), blank=True)
    rating = models.FloatField(
        _("rating"),
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
    )
    type = models.CharField(_("type"), max_length=255, choices=Types.choices)
    genre = models.ManyToManyField(Genre, through="GenreFilmwork")
    person = models.ManyToManyField(Person, through="PersonFilmwork")
    # certificate = models.CharField(
    #     _("certificate"), max_length=512, blank=True
    # )
    # Параметр upload_to указывает, в какой подпапке будут храниться загружемые
    # файлы. Базовая папка указана в файле настроек как MEDIA_ROOT
    file_path = models.FileField(
        _("file"), blank=True, null=True, upload_to="movies/"
    )

    def __str__(self) -> str:
        return self.title

    class Meta:
        db_table = 'content"."film_work'
        verbose_name = "Фильм"
        verbose_name_plural = "Фильмы"


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey("Filmwork", on_delete=models.CASCADE)
    genre = models.ForeignKey("Genre", on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'content"."genre_film_work'
        constraints = [
            models.UniqueConstraint(
                fields=["film_work", "genre"], name="unique_genre"
            )
        ]


class PersonFilmwork(UUIDMixin):
    person = models.ForeignKey("Person", on_delete=models.CASCADE)
    film_work = models.ForeignKey("Filmwork", on_delete=models.CASCADE)
    role = models.CharField("role", max_length=255)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'content"."person_film_work'
        constraints = [
            models.UniqueConstraint(
                fields=["film_work", "person", "role"],
                name="unique_person_role",
            )
        ]
