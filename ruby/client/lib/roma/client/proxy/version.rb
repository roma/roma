# -*- coding: utf-8 -*-
#
# = roma/client/proxy/version.rb
# This file is derived from roma client proxy daemon.
#
module Roma #:nodoc:
    module Client #:nodoc:
      # == What Is This Library?
      # ROMA client proxy daemon's version module
      #
      module VERSION
        # メジャーバージョン
        MAJOR = 0

        # マイナバージョン
        MINOR = 1

        # TINY version
        TINY  = 0

        # バージョン文字列
        STRING = [MAJOR, MINOR, TINY].join('.')
      end
    end
end
