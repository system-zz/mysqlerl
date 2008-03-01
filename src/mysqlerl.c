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

typedef u_int32_t msglen_t;
typedef enum {
  QUERY_MSG = 0, COMMIT_MSG = 1, ROLLBACK_MSG = 2, EXTENDED_MSG = 255
} msgtype_t;

struct msg {
  msgtype_t type;
  char *msg;
  size_t msglen;
  char *buf;
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
restartable_read(char *buf, size_t buflen)
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
restartable_write(const char *buf, size_t buflen)
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
  if (restartable_read((char *)&len, sizeof(len)) == -1) {
    logmsg("ERROR: couldn't read %d byte message prefix: %s.",
           sizeof(len), strerror(errno));

    free(msg);
    exit(2);
  }
  len = ntohl(len);

  msg->buf = malloc(len + 1);
  if (msg->buf == NULL) {
    logmsg("ERROR: Couldn't malloc %d bytes: %s.", len,
           strerror(errno));

    free(msg);
    exit(2);
  }
  msg->buf[len] = '\0';

  logmsg("DEBUG: reading message body (len: %d).", len);
  if (restartable_read(msg->buf, len) == -1) {
    logmsg("ERROR: couldn't read %d byte message: %s.",
           len, strerror(errno));

    free(msg->buf);
    free(msg);
    exit(2);
  }

  msg->type   = msg->buf[0];
  msg->msg    = msg->buf + 1;
  msg->msglen = len - 1;

  return msg;
}

int
write_cmd(const char *cmd, msglen_t len)
{
  msglen_t nlen;

  nlen = htonl(len + 3);
  if (restartable_write((char *)&nlen, sizeof(nlen)) == -1)
    return -1;
  if (restartable_write(" - ", 3) == -1)
    return -1;
  if (restartable_write(cmd, len) == -1)
    return -1;

  return 0;
}

void
dispatch_db_cmd(MYSQL *dbh, msg_t *msg)
{
  switch (msg->type) {
  case QUERY_MSG:
    logmsg("DEBUG: got query msg: %s.", msg->msg);
    write_cmd(msg->msg, msg->msglen);
    break;

  default:
    logmsg("WARNING: message type %d unknown.", msg->type);
    exit(3);
  }
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
    free(msg->buf);
    free(msg);
  }

  mysql_close(&dbh);

  logmsg("INFO: shutting down.");
  closelog();

  return 0;
}
