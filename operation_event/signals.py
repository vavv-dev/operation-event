import json
import logging
import sys

from common.djangoapps.student.models import CourseAccessRole, user_by_anonymous_id
from common.djangoapps.student.models import CourseEnrollment, UserProfile
from completion.models import BlockCompletion
from crum import get_current_request, get_current_user
from crum import get_current_request
from django.contrib.auth.models import User
from django.contrib.sites.models import Site
from django.db import models, transaction
from django.db.models.manager import Manager
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver
from django.utils.timezone import localtime
from edx_proctoring.models import ProctoredExamStudentAttempt
from ipware import get_client_ip
from lms.djangoapps.course_api.blocks.api import get_blocks
from lms.djangoapps.grades.signals.signals import SUBSECTION_SCORE_CHANGED
from openedx.core.djangoapps.content.course_overviews.models import CourseOverview
from openedx.core.djangoapps.django_comment_common.signals import (
    comment_created,
    comment_deleted,
    comment_edited,
    comment_voted,
    thread_created,
    thread_deleted,
    thread_edited,
    thread_voted,
)
from openedx.core.djangoapps.signals.signals import COURSE_GRADE_CHANGED
from submissions.models import Score, Submission
from xmodule.modulestore.django import modulestore


# log to stderr
logger = logging.getLogger(__name__)
stderr_handler = logging.StreamHandler(stream=sys.stderr)
stderr_handler.setFormatter(logging.Formatter("%(message)s"))
logger.setLevel(logging.INFO)
logger.addHandler(stderr_handler)


namespace = __name__


event_fields = {
    CourseOverview: [
        "created",
        "modified",
        "id",
        "display_name",
        "invitation_only",
        "course_image_url",
        "effort",
        "visible_to_staff_only",
        "start",
        "end",
        "enrollment_start",
        "enrollment_end",
        "certificate_available_date",
        "pacing",
    ],
    CourseAccessRole: [
        "id",
        "user_id",
        "course_id",
        "org",
        "role",
    ],
    User: [
        "id",
        "username",
        "email",
        "is_active",
        "is_staff",
        "is_superuser",
        "last_login",
        "date_joined",
    ],
    UserProfile: [
        "id",
        "user_id",
        "name",
        "year_of_birth",
        "gender",
    ],
    CourseEnrollment: [
        "created",
        "id",
        "user_id",
        "course_id",
        "mode",
        "is_active",
    ],
    BlockCompletion: [
        "user_id",
        "context_key",
        "block_key",
    ],
    ProctoredExamStudentAttempt: [
        "created",
        "modified",
        "id",
        "user_id",
        "status",
        "proctored_exam__course_id",
        "proctored_exam__content_id",
        "proctored_exam__is_active",
    ],
    Submission: [
        "id",
        "uuid",
        "student_item__student_id",
        "student_item__course_id",
        "student_item__item_id",
        "attempt_number",
        "submitted_at",
        "created_at",
        "answer",
        "status",
    ],
    Score: [
        "id",
        "submission__uuid",
        "points_earned",
        "points_possible",
        "created_at",
        "reset",
    ],
}


@receiver(post_save, sender=CourseOverview)
@receiver(post_save, sender=CourseAccessRole)
@receiver(post_delete, sender=CourseAccessRole)
@receiver(post_save, sender=User)
@receiver(post_save, sender=UserProfile)
@receiver(post_save, sender=CourseEnrollment)
@receiver(post_save, sender=ProctoredExamStudentAttempt)
@receiver(post_delete, sender=ProctoredExamStudentAttempt)
@receiver(post_save, sender=Score)
@receiver(post_save, sender=Site)
def emit_model_event(sender, instance, created=None, signal=None, **kwargs):
    """emit_model_event.

    :param sender:
    :param instance:
    :param created:
    :param signal:
    :param kwargs:
    """
    message = _model_to_dict(instance, event_fields[sender])
    _emit_event(sender, message, created=created, deleted=signal is post_delete)


"""
Forum Thread, Comment
"""


@receiver(comment_created)
@receiver(comment_edited)
@receiver(comment_voted)
@receiver(comment_deleted)
@receiver(thread_created)
@receiver(thread_edited)
@receiver(thread_voted)
@receiver(thread_deleted)
def emit_forumpost_event(sender, post, signal=None, **kwargs):
    """emit_forumpost_event.

    :param sender:
    :param post:
    :param signal:
    :param kwargs:
    """
    message = post.to_dict()

    user = get_current_user()
    if user and user.is_authenticated:
        message["username"] = user.username

    _emit_event(
        "ForumPost",
        message,
        created=signal in [comment_created, thread_created],
        deleted=signal in [comment_deleted, thread_deleted],
    )


"""
Grade and Completion
"""


@receiver(COURSE_GRADE_CHANGED)
def emit_coursegrade_event(sender, user, course_grade, course_key, **kwargs):
    """emit_coursegrade_event.

    :param sender:
    :param user:
    :param course_grade:
    :param course_key:
    :param kwargs:
    """
    grade_summary = {}

    if course_grade.attempted:
        course_data = course_grade.course_data
        course = course_data.course
        grade_summary = {
            grader.get("type"): {
                "min_count": grader.get("min_count"),
                "weight": grader.get("weight"),
            }
            for grader in course.raw_grader
        }

        # https://github.com/openedx/edx-platform/pull/30043/commits
        # a162140492d256be7cde5a53cb24ba221bc5cf5b
        #
        # graded_squentials = course_grade.graded_subsections_by_format(False)
        # for subgrader, _format, weight in course.grader.subgraders:
        #     subgrade_result = subgrader.grade(graded_squentials)
        #     grade_summary[_format].update(
        #         percent=subgrade_result.get("percent"),
        #         weighted_percent=weight * subgrade_result.get("percent"),
        #      )

        subsections_by_format = course_grade.graded_subsections_by_format
        for subgrader, _format, weight in course.grader.subgraders:
            subgrade_result = subgrader.grade(subsections_by_format)
            grade_summary[_format].update(
                percent=subgrade_result.get("percent"),
                weighted_percent=weight * subgrade_result.get("percent"),
            )

    message = {
        "username": user.username,
        "course_id": str(course_key),
        "percent_grade": course_grade.percent,
        "letter_grade": course_grade.letter_grade,
        "passed": course_grade.passed,
        "grade_summary": grade_summary,
    }
    _emit_event("CourseGrade", message)


@receiver(SUBSECTION_SCORE_CHANGED)
def emit_subsectiongrade_event(sender, course, course_structure, user, subsection_grade, **kwargs):
    """emit_subsectiongrade_event.

    :param sender:
    :param course:
    :param course_structure:
    :param user:
    :param subsection_grade:
    :param kwargs:
    """
    message = {
        "username": user.username,
        "course_id": str(course.id),
        "usage_key": str(subsection_grade.location),
        "earned_all": subsection_grade.all_total.earned,
        "possible_all": subsection_grade.all_total.possible,
        "earned_graded": subsection_grade.graded_total.earned,
        "possible_graded": subsection_grade.graded_total.possible,
    }

    _emit_event("SubsectionGrade", message)


@receiver(post_save, sender=BlockCompletion)
def emit_blockcompletion_event(sender, instance, **kwargs):
    """emit_blockcompletion_event.

    :param sender:
    :param instance:
    :param kwargs:
    """
    if instance.completion < 1.0:
        return

    # get subsection
    def get_subsection_location(location):
        """get_subsection_location.

        :param location:
        """
        parent_location = modulestore().get_parent_location(location)
        if parent_location.block_type == "sequential":  # type: ignore
            return parent_location
        return get_subsection_location(parent_location)

    subsection_usage_key = get_subsection_location(instance.block_key)

    blocks = get_blocks(
        get_current_request(),
        subsection_usage_key,
        instance.user,
        nav_depth=2,
        requested_fields=["complete", "completion", "due", "special_exam_info"],
        block_types_filter=["sequential"],
    )

    # subsection complete
    subsection_block = blocks["blocks"][blocks["root"]]
    subsection_complete = subsection_block.get("complete", False)

    message = _model_to_dict(instance, event_fields[sender])
    message.update(
        subsection_usage_key=str(subsection_usage_key),
        subsection_complete=subsection_complete,
        due=str(subsection_block.get("due") or ""),
    )
    _emit_event(sender, message)


@receiver(post_save, sender=Submission)
def emit_submission_event(sender, instance, created=None, **kwargs):
    """emit_submission_event.

    :param sender:
    :param instance:
    :param created:
    :param kwargs:
    """
    message = _model_to_dict(instance, event_fields[sender])
    uuid = message["student_item"]["student_id"]
    message.update(username=user_by_anonymous_id(uuid).username)
    _emit_event(sender, message, created=created)


def _emit_event(sender, message, created=None, deleted=None):
    """_emit_event.

    :param sender:
    :param message:
    :param created:
    :param deleted:
    """
    # event type
    sender = sender if isinstance(sender, str) else sender.__name__
    event = {
        "event_type": f"{namespace}.{sender.lower()}",
        "message": message,
        "created": created,
        "deleted": deleted,
        "time": str(localtime()),
    }

    request = get_current_request()
    event["client_ip"], _ = get_client_ip(request) if request else (None, None)
    event["request_username"] = request.user.username if request and request.user else None
    event["user_agent"] = request.META.get("HTTP_USER_AGENT", None) if request else None

    def emit(e):
        """emit.

        :param e:
        """
        e = json.dumps(e)
        logger.info(e)

    if transaction.get_connection().in_atomic_block:
        transaction.on_commit(lambda: emit(event))
    else:
        emit(event)


def _model_to_dict(instance, field_names=None, related_model_cache=None):
    """_model_to_dict.

    :param instance:
    :param field_names:
    :param related_model_cache:
    """
    if field_names is None:
        field_names = [f.name for f in instance._meta.get_fields()]

    if related_model_cache is None:
        related_model_cache = {}

    result = {}

    for field_name in field_names:
        parts = field_name.split("__", 1)
        key = parts[0]

        # cache
        value = related_model_cache.get(key)
        if not value:
            value = getattr(instance, key, None)
            related_model_cache[key] = value

        if len(parts) == 1:
            if isinstance(value, Manager):
                continue

            # attach username
            if key == "user_id":
                if hasattr(instance, "user"):
                    result["username"] = instance.user.username
                    continue

            if isinstance(value, models.Model) and value:
                key = f"{key}_id"
                value = value.pk

            try:
                # test json serializable
                json.dumps(value)
                result[key] = value
            except:
                result[key] = str(value)

        else:
            child_key = parts[1]
            nested_value = _model_to_dict(value, [child_key], related_model_cache)
            result.setdefault(key, {}).update(nested_value)

    return result
