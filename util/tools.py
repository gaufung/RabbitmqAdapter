# -*- coding:utf-8 -*-
from __future__ import unicode_literals


def remove_punctuations(content):
    """
    remove punctuations in the content
    :param content: to refine content
    :return: refined content
    """
    punctuations_set = set("~.。?？！!:：…～﹏\"\'`”“·•°\t\n;、/,，$=《》；")
    if not isinstance(content, unicode):
        content = content.decode("utf-8")
    refine_characters = [c if c not in punctuations_set else "" for c in content]
    return "".join(refine_characters)
