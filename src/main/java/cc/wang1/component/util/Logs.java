package cc.wang1.component.util;

import ch.qos.logback.classic.LoggerContext;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.util.Arrays;
import java.util.Objects;

public final class Logs {

    private Logs() {
    }

    private static Logger get() {
        ILoggerFactory iLoggerFactory = LoggerFactory.getILoggerFactory();
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        if (iLoggerFactory instanceof LoggerContext) {
            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            if (!loggerContext.getFrameworkPackages().contains(Logs.class.getName())) {
                loggerContext.getFrameworkPackages().add(Logs.class.getName());
            }
            return loggerContext.getLogger(stackTrace[3].getClassName());
        }
        return iLoggerFactory.getLogger(stackTrace[3].getClassName());
    }

    private static String getLogContentTemplate() {
        return "%s";
    }

    private static String getMsgByTemplate(String msg) {
        return msg;
    }

    public static String getName() {
        return get().getName();
    }

    private static boolean isTraceEnabled() {
        return get().isTraceEnabled();
    }

    public static void trace(String msg) {
        if (isTraceEnabled()) {
            get().trace(getMsgByTemplate(msg));
        }
    }

    public static void trace(String format, Object arg) {
        if (isTraceEnabled()) {
            get().trace(getMsgByTemplate(format), transfer(arg));
        }
    }

    public static void trace(String format, Object arg1, Object arg2) {
        if (isTraceEnabled()) {
            get().trace(getMsgByTemplate(format), transfer(arg1), transfer(arg2));
        }
    }

    public static void trace(String format, Object... arguments) {
        if (isTraceEnabled()) {
            get().trace(getMsgByTemplate(format), transfer(arguments));
        }
    }

    public static void trace(String msg, Throwable t) {
        if (isTraceEnabled()) {
            get().trace(getMsgByTemplate(msg), t);
        }
    }

    private static boolean isTraceEnabled(Marker marker) {
        return get().isTraceEnabled(marker);
    }

    public static void trace(Marker marker, String msg) {
        if (isTraceEnabled(marker)) {
            get().trace(marker, getMsgByTemplate(msg));
        }
    }

    public static void trace(Marker marker, String format, Object arg) {
        if (isTraceEnabled(marker)) {
            get().trace(marker, getMsgByTemplate(format), transfer(arg));
        }
    }

    public static void trace(Marker marker, String format, Object arg1, Object arg2) {
        if (isTraceEnabled(marker)) {
            get().trace(marker, getMsgByTemplate(format), transfer(arg1), transfer(arg2));
        }
    }

    public static void trace(Marker marker, String format, Object... arguments) {
        if (isTraceEnabled(marker)) {
            get().trace(marker, getMsgByTemplate(format), transfer(arguments));
        }
    }

    public static void trace(Marker marker, String msg, Throwable t) {
        if (isTraceEnabled(marker)) {
            get().trace(marker, getMsgByTemplate(msg), t);
        }
    }

    private static boolean isDebugEnabled() {
        return get().isDebugEnabled();
    }

    public static void debug(String msg) {
        if (isDebugEnabled()) {
            get().debug(getMsgByTemplate(msg));
        }
    }

    public static void debug(String format, Object arg) {
        if (isDebugEnabled()) {
            get().debug(getMsgByTemplate(format), transfer(arg));
        }
    }

    public static void debug(String format, Object arg1, Object arg2) {
        if (isDebugEnabled()) {
            get().debug(getMsgByTemplate(format), transfer(arg1), transfer(arg2));
        }
    }

    public static void debug(String format, Object... arguments) {
        if (isDebugEnabled()) {
            get().debug(getMsgByTemplate(format), transfer(arguments));
        }
    }

    public static void debug(String msg, Throwable t) {
        if (isDebugEnabled()) {
            get().debug(getMsgByTemplate(msg), t);
        }
    }

    private static boolean isDebugEnabled(Marker marker) {
        return get().isDebugEnabled(marker);
    }

    public static void debug(Marker marker, String msg) {
        if (isDebugEnabled(marker)) {
            get().debug(marker, getMsgByTemplate(msg));
        }
    }

    public static void debug(Marker marker, String format, Object arg) {
        if (isDebugEnabled(marker)) {
            get().debug(marker, getMsgByTemplate(format), transfer(arg));
        }
    }

    public static void debug(Marker marker, String format, Object arg1, Object arg2) {
        if (isDebugEnabled(marker)) {
            get().debug(marker, getMsgByTemplate(format), transfer(arg1), transfer(arg2));
        }
    }

    public static void debug(Marker marker, String format, Object... arguments) {
        if (isDebugEnabled(marker)) {
            get().debug(marker, getMsgByTemplate(format), transfer(arguments));
        }
    }

    public static void debug(Marker marker, String msg, Throwable t) {
        if (isDebugEnabled(marker)) {
            get().debug(marker, getMsgByTemplate(msg), t);
        }
    }

    private static boolean isInfoEnabled() {
        return get().isInfoEnabled();
    }

    public static void info(String msg) {
        if (isInfoEnabled()) {
            get().info(getMsgByTemplate(msg));
        }
    }

    public static void info(String format, Object arg) {
        if (isInfoEnabled()) {
            get().info(getMsgByTemplate(format), transfer(arg));
        }
    }

    public static void info(String format, Object arg1, Object arg2) {
        if (isInfoEnabled()) {
            get().info(getMsgByTemplate(format), transfer(arg1), transfer(arg2));
        }
    }

    public static void info(String format, Object... arguments) {
        if (isInfoEnabled()) {
            get().info(getMsgByTemplate(format), transfer(arguments));
        }
    }

    public static void info(String msg, Throwable t) {
        if (isInfoEnabled()) {
            get().info(getMsgByTemplate(msg), t);
        }
    }

    private static boolean isInfoEnabled(Marker marker) {
        return get().isDebugEnabled(marker);
    }

    public static void info(Marker marker, String msg) {
        if (isInfoEnabled(marker)) {
            get().info(getMsgByTemplate(msg));
        }
    }

    public static void info(Marker marker, String format, Object arg) {
        if (isInfoEnabled(marker)) {
            get().info(marker, getMsgByTemplate(format), transfer(arg));
        }
    }

    public static void info(Marker marker, String format, Object arg1, Object arg2) {
        if (isInfoEnabled(marker)) {
            get().info(marker, getMsgByTemplate(format), transfer(arg1), transfer(arg2));
        }
    }

    public static void info(Marker marker, String format, Object... arguments) {
        if (isInfoEnabled(marker)) {
            get().info(marker, getMsgByTemplate(format), transfer(arguments));
        }
    }

    private static Object transfer(Object arg) {
        if (arg instanceof Throwable) {
            return arg;
        } else {
            return Jsons.toJson(arg);
        }
    }

    private static Object[] transfer(Object[] arguments) {
        Object[] args = null;
        if (Objects.nonNull(arguments) && arguments.length > 0) {
            args = Arrays.stream(arguments)
                    .map(Logs::transfer).toArray();
        }
        return args;
    }

    public static void info(Marker marker, String msg, Throwable t) {
        if (isInfoEnabled(marker)) {
            get().info(marker, getMsgByTemplate(msg), t);
        }
    }

    private static boolean isWarnEnabled() {
        return get().isWarnEnabled();
    }

    public static void warn(String msg) {
        if (isWarnEnabled()) {
            get().warn(getMsgByTemplate(msg));
        }
    }

    public static void warn(String format, Object arg) {
        if (isWarnEnabled()) {
            get().warn(getMsgByTemplate(format), transfer(arg));
        }
    }

    public static void warn(String format, Object... arguments) {
        if (isWarnEnabled()) {
            get().warn(getMsgByTemplate(format), transfer(arguments));
        }
    }

    public static void warn(String format, Object arg1, Object arg2) {
        if (isWarnEnabled()) {
            get().warn(getMsgByTemplate(format), transfer(arg1), transfer(arg2));
        }
    }

    public static void warn(String msg, Throwable t) {
        if (isWarnEnabled()) {
            get().warn(getMsgByTemplate(msg), t);
        }
    }

    private static boolean isWarnEnabled(Marker marker) {
        return get().isWarnEnabled(marker);
    }

    public static void warn(Marker marker, String msg) {
        if (isWarnEnabled(marker)) {
            get().warn(marker, getMsgByTemplate(msg));
        }
    }

    public static void warn(Marker marker, String format, Object arg) {
        if (isWarnEnabled(marker)) {
            get().warn(marker, getMsgByTemplate(format), transfer(arg));
        }
    }

    public static void warn(Marker marker, String format, Object arg1, Object arg2) {
        if (isWarnEnabled(marker)) {
            get().warn(marker, getMsgByTemplate(format), transfer(arg1), transfer(arg2));
        }
    }

    public static void warn(Marker marker, String format, Object... arguments) {
        if (isWarnEnabled(marker)) {
            get().warn(marker, getMsgByTemplate(format), transfer(arguments));
        }
    }

    public static void warn(Marker marker, String msg, Throwable t) {
        if (isWarnEnabled(marker)) {
            get().warn(marker, getMsgByTemplate(msg), t);
        }
    }

    private static boolean isErrorEnabled() {
        return get().isErrorEnabled();
    }

    public static void error(String msg) {
        if (isErrorEnabled()) {
            get().error(getMsgByTemplate(msg));
        }
    }

    public static void error(String format, Object arg) {
        if (isErrorEnabled()) {
            get().error(getMsgByTemplate(format), transfer(arg));
        }
    }

    public static void error(String format, Object arg1, Object arg2) {
        get().error(getMsgByTemplate(format), transfer(arg1), transfer(arg2));
    }

    public static void error(String format, Object... arguments) {
        get().error(getMsgByTemplate(format), transfer(arguments));
    }

    public static void error(String msg, Throwable t) {
        if (isErrorEnabled()) {
            get().error(getMsgByTemplate(msg), t);
        }
    }

    private static boolean isErrorEnabled(Marker marker) {
        return get().isErrorEnabled(marker);
    }

    public static void error(Marker marker, String msg) {
        if (isErrorEnabled(marker)) {
            get().error(marker, getMsgByTemplate(msg));
        }
    }

    public static void error(Marker marker, String format, Object arg) {
        if (isErrorEnabled(marker)) {
            get().error(marker, getMsgByTemplate(format), transfer(arg));
        }
    }

    public static void error(Marker marker, String format, Object arg1, Object arg2) {
        if (isErrorEnabled(marker)) {
            get().error(marker, getMsgByTemplate(format), transfer(arg1), transfer(arg2));
        }
    }

    public static void error(Marker marker, String format, Object... arguments) {
        if (isErrorEnabled(marker)) {
            get().error(marker, getMsgByTemplate(format), transfer(arguments));
        }
    }

    public static void error(Marker marker, String msg, Throwable t) {
        if (isErrorEnabled(marker)) {
            get().error(marker, getMsgByTemplate(msg), t);
        }
    }

}