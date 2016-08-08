# Copyright 2016, Google Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
#     * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
# copyright notice, this list of conditions and the following disclaimer
# in the documentation and/or other materials provided with the
# distribution.
#     * Neither the name of Google Inc. nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""Shared implementation."""

import logging
import threading
import time

import six

import grpc

_EMPTY_METADATA = cygrpc.Metadata(())


def encode(s):
  if isinstance(s, bytes):
    return s
  else:
    return s.encode('ascii')


def decode(b):
  if isinstance(b, str):
    return b
  else:
    try:
      return b.decode('utf8')
    except UnicodeDecodeError:
      logging.exception('Invalid encoding on {}'.format(b))
      return b.decode('latin1')


def application_metadata(cygrpc_metadata):
  if cygrpc_metadata is None:
    return ()
  else:
    return tuple(
        (decode(key), value if key[-4:] == b'-bin' else decode(value))
        for key, value in cygrpc_metadata)


def _transform(message, transformer, exception_message):
  if transformer is None:
    return message
  else:
    try:
      return transformer(message)
    except Exception:  # pylint: disable=broad-except
      logging.exception(exception_message)
      return None


def serialize(message, serializer):
  return _transform(message, serializer, 'Exception serializing message!')


def deserialize(serialized_message, deserializer):
  return _transform(serialized_message, deserializer,
                    'Exception deserializing message!')


def fully_qualified_method(group, method):
  return '/{}/{}'.format(group, method)


class CleanupThread(threading.Thread):
  """A threading.Thread subclass supporting custom behavior on join().

  On Python Interpreter exit, Python will attempt to join outstanding threads
  prior to garbage collection.  We may need to do additional cleanup, and
  we accomplish this by overriding the join() method.
  """

  def __init__(self, behavior, group=None, target=None, name=None,
               args=(), kwargs={}):
    """Constructor.

    Args:
      behavior (function): Function called on join() with a single
          argument, timeout, indicating the maximum duration of
          `behavior`, or None indicating `behavior` has no deadline.
          `behavior` must be idempotent.
      group (None): should be None.  Reseved for future extensions
          when ThreadGroup is implemented.
      target (function): The function to invoke when this thread is
          run.  Defaults to None.
      name (str): The name of this thread.  Defaults to None.
        args (tuple[object]): A tuple of arguments to pass to `target`.
      kwargs (dict[str,object]): A dictionary of keyword arguments to
           pass to `target`.
    """
    super(CleanupThread, self).__init__(group=group, target=target,
                                        name=name, args=args, kwargs=kwargs)
    self._behavior = behavior

  def join(self, timeout=None):
    start_time = time.time()
    self._behavior(timeout)
    end_time = time.time()
    if timeout is not None:
      timeout -= end_time - start_time
      timeout = max(timeout, 0)
    super(CleanupThread, self).join(timeout)
