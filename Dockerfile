FROM ruby:3.0

ARG VERSION

RUN gem install pg_online_schema_change -v $VERSION
