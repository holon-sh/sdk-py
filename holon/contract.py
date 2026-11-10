"""The generated-client contract this runtime reads.

``holon types generate`` and ``holon types check`` read ``CONTRACT_VERSION`` and refuse clients of
another contract, so a project is told to upgrade holon-sh or holon instead of getting clients its
runtime cannot read. It imports nothing, so reading it is quick. From contract 3, each app's client
has its events at ``ext.events``.
"""

CONTRACT_VERSION = 3
