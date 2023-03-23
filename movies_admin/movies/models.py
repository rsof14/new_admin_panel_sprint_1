import uuid
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _('Genre')
        verbose_name_plural = _('Genres')


class Gender(models.TextChoices):
    MALE = 'male', _('male')
    FEMALE = 'female', _('female')


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('full_name'), max_length=255)
    gender = models.TextField(_('gender'), choices=Gender.choices)

    def __str__(self):
        return self.full_name

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('Person')
        verbose_name_plural = _('Persons')


class Filmwork(UUIDMixin, TimeStampedMixin):
    title = models.CharField(_('title'), max_length=255)
    description = models.TextField(_('description'), blank=True)
    creation_date = models.DateField(_('creation_date'))
    rating = models.FloatField(_('rating'), blank=True,
                               validators=[MinValueValidator(0), MaxValueValidator(100)])
    TYPE_CHOICES = (("MOVIE", _("Movie")), ("TV SHOW", _("TV Show")))
    type = models.CharField(_('type'), max_length=10, choices=TYPE_CHOICES, default="MOVIE")
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')
    persons = models.ManyToManyField(Person, through='PersonFilmwork')
    certificate = models.CharField(_('certificate'), max_length=512, blank=True)
    file_path = models.FileField(_('file'), blank=True, upload_to='movies/')

    def __str__(self):
        return self.title

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['title', 'type', 'creation_date'], name='film_work_idx')
        ]
        db_table = "content\".\"film_work"
        verbose_name = _('Filmwork')
        verbose_name_plural = _('Filmworks')


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    genre = models.ForeignKey('Genre', on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return str(_("genre"))

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['film_work_id', 'genre_id'], name='film_work_genre_idx')
        ]
        db_table = "content\".\"genre_film_work"
        verbose_name = _('Genre in filmwork')
        verbose_name_plural = _('Genres in filmwork')


class Role(models.TextChoices):
    ACTOR = 'actor', _('actor')
    DIRECTOR = 'director', _('director')
    WRITER = 'writer', _('writer')


class PersonFilmwork(UUIDMixin):
    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    person = models.ForeignKey('Person', on_delete=models.CASCADE)
    role = models.TextField(_('role'), choices=Role.choices)
    created = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return str(_("person"))

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['film_work_id', 'person_id'], name='film_work_person_idx')
        ]
        db_table = "content\".\"person_film_work"
        verbose_name = _('Person in filmwork')
        verbose_name_plural = _('Persons in filmwork')
