# -*- coding: utf-8 -*-
#
# = roma/commons/version.rb
# This file is derived from roma client.
#
module Roma #:nodoc:
    module Commons #:nodoc:
      # == What Is This Library?
      # ROMA Commons バージョンモジュール
      #
      module VERSION
        # メジャーバージョン
        MAJOR = 0

        # マイナバージョン
        MINOR = 3

        # TINY version
        TINY  = 6

        # バージョン文字列
        STRING = [MAJOR, MINOR, TINY].join('.')
      end
    end
end
