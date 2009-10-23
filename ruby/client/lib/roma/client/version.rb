# -*- coding: utf-8 -*-
#
# = roma/client/version.rb
# This file is derived from roma client.
#
module Roma #:nodoc:
    module Client #:nodoc:
      # == What Is This Library?
      # ROMA クライアントバージョン情報モジュール
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
