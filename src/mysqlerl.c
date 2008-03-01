/*
 * MySQL port driver.
 *
 * Copyright (C) 2008, Brian Cully <bjc@kublai.com>
 */

#include <erl_interface.h>
#include <ei.h>
#include <mysql.h>

#include <errno.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

const char *LOGPATH = "/tmp/mysqlerl.log";
static FILE *logfile = NULL;

const char *QUERY_MSG = "sql_query";

typedef u_int32_t msglen_t;

struct msg {
  ETERM *cmd;
  unsigned char *buf;
};
typedef struct msg msg_t;

void
openlog()
{
  logfile = fopen(LOGPATH, "a");
}

void
closelog()
{
  fclose(logfile);
}

void
logmsg(const char *format, ...)
{
  FILE *out = logfile;
  va_list args;

  if (logfile == NULL)
    out = stderr;

  va_start(args, format);
  (void)vfprintf(out, format, args);
  (void)fprintf(out, "\n");
  va_end(args);

  fflush(out);
}

int
restartable_read(unsigned char *buf, size_t buflen)
{
  ssize_t rc, readb;

  rc = 0;
  READLOOP:
  while (rc < buflen) {
    readb = read(STDIN_FILENO, buf + rc, buflen - rc);
    if (readb == -1) {
      if (errno == EAGAIN || errno == EINTR)
        goto READLOOP;

      return -1;
    } else if (readb == 0) {
      logmsg("ERROR: EOF trying to read additional %d bytes from "
             "standard input", buflen - rc);
      return -1;
    }

    rc += readb;
  }

  return rc;
}

int
restartable_write(const unsigned char *buf, size_t buflen)
{
  ssize_t rc, wroteb;

  rc = 0;
  WRITELOOP:
  while (rc < buflen) {
    wroteb = write(STDOUT_FILENO, buf + rc, buflen - rc);
    if (wroteb == -1) {
      if (errno == EAGAIN || errno == EINTR)
        goto WRITELOOP;

      return -1;
    }

    rc += wroteb;
  }

  return rc;
}

msg_t *
read_msg()
{
  msg_t *msg;
  msglen_t len;

  msg = (msg_t *)malloc(sizeof(msg_t));
  if (msg == NULL) {
    logmsg("ERROR: Couldn't allocate message for reading: %s.\n",
           strerror(errno));

    exit(2);
  }

  logmsg("DEBUG: reading message length.");
  if (restartable_read((unsigned char *)&len, sizeof(len)) == -1) {
    logmsg("ERROR: couldn't read %d byte message prefix: %s.",
           sizeof(len), strerror(errno));

    free(msg);
    exit(2);
  }

  len = ntohl(len);
  msg->buf = malloc(len);
  if (msg->buf == NULL) {
    logmsg("ERROR: Couldn't malloc %d bytes: %s.", len,
           strerror(errno));

    free(msg);
    exit(2);
  }

  logmsg("DEBUG: reading message body (len: %d).", len);
  if (restartable_read(msg->buf, len) == -1) {
    logmsg("ERROR: couldn't read %d byte message: %s.",
           len, strerror(errno));

    free(msg->buf);
    free(msg);
    exit(2);
  }

  msg->cmd = erl_decode(msg->buf);

  return msg;
}

int
write_cmd(const char *cmd, msglen_t len)
{
  msglen_t nlen;

  nlen = htonl(len + 3);
  if (restartable_write((unsigned char *)&nlen, sizeof(nlen)) == -1)
    return -1;
  if (restartable_write((unsigned char *)" - ", 3) == -1)
    return -1;
  if (restartable_write((unsigned char *)cmd, len) == -1)
    return -1;

  return 0;
}

void
dispatch_db_cmd(MYSQL *dbh, msg_t *msg)
{
  ETERM *tag;

  tag = erl_element(1, msg->cmd);
  if (strncmp((char *)ERL_ATOM_PTR(tag), QUERY_MSG, sizeof(QUERY_MSG)) == 0) {
    ETERM *query;
    char *q;
    query = erl_element(2, msg->cmd);
    q = erl_iolist_to_string(query);
    erl_free_term(query);

    logmsg("DEBUG: got query msg: %s.", q);
    write_cmd(q, strlen(q));
    erl_free(q);
  } else {
    logmsg("WARNING: message type %s unknown.", (char *)ERL_ATOM_PTR(tag));
    erl_free_term(tag);
    exit(3);
  }

  erl_free_term(tag);
}

void
usage()
{
  fprintf(stderr, "Usage: mysqlerl host port db_name user passwd\n");
  exit(1);
}

int
main(int argc, char *argv[])
{
  MYSQL dbh;
  char *host, *port, *db_name, *user, *passwd;
  msg_t *msg;

  openlog();
  logmsg("INFO: starting up.");

  if (argc < 6)
    usage();

  host    = argv[1];
  port    = argv[2];
  db_name = argv[3];
  user    = argv[4];
  passwd  = argv[5];

  erl_init(NULL, 0);

  mysql_init(&dbh);
  if (mysql_real_connect(&dbh, host, user, passwd,
                         db_name, atoi(port), NULL, 0) == NULL) {
    logmsg("ERROR: Failed to connect to database %s: %s (%s:%s).",
           db_name, mysql_error(&dbh), user, passwd);
    exit(2);
  }

  while ((msg = read_msg()) != NULL) {
    dispatch_db_cmd(&dbh, msg);

    /* XXX: Move this to function */
    erl_free_compound(msg->cmd);
    free(msg->buf);
    free(msg);
  }

  mysql_close(&dbh);

  logmsg("INFO: shutting down.");
  closelog();

  return 0;
}
