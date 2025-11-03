package server

import (
	"context"
	"fmt"
	"go-noti-server/internal/datastore"
	"go-noti-server/internal/telemetry"
	pbh "go-noti-server/protos/health"
	pb "go-noti-server/protos/notifications"
	"net"
	"os"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/newrelic/go-agent/v3/integrations/nrgrpc"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type server struct {
	pb.UnimplementedNotificationServiceServer
}

type healthCheckServer struct {
	pbh.UnimplementedHealthServiceServer
}

func RunGrpcServer() {
	var (
		port     = os.Getenv("PORT")
		lis, err = net.Listen("tcp", port)
		s        = grpc.NewServer(grpc.ChainUnaryInterceptor(nrgrpc.UnaryServerInterceptor(telemetry.App), AuthInterceptor))
	)

	pb.RegisterNotificationServiceServer(s, &server{})
	pbh.RegisterHealthServiceServer(s, &healthCheckServer{})

	telemetry.Log(zerolog.InfoLevel, fmt.Sprintf("server listening at %v", lis.Addr()))

	if err != nil {
		telemetry.Log(zerolog.FatalLevel, fmt.Sprintf("failed to listen: %v", err))
	}

	if err := s.Serve(lis); err != nil {
		telemetry.Log(zerolog.FatalLevel, fmt.Sprintf("failed to serve: %v", err))
	}
}

func (s *server) SendMessage(ctx context.Context, req *pb.NotificationRequest) (*pb.NotificationResponse, error) {
	start := time.Now()
	txn := newrelic.FromContext(ctx)
	notification := req.GetNotification()
	log(ctx, notification, zerolog.InfoLevel, "Notification request received")

	seg := txn.StartSegment("MarshallingNotification")
	marshaller := proto.MarshalOptions{Deterministic: true}
	data, err := marshaller.Marshal(notification)
	if err != nil {
		msg := fmt.Sprintf("error marshalling notification: %v", err)
		log(ctx, notification, zerolog.ErrorLevel, msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	log(ctx, notification, zerolog.DebugLevel, "notification marshalled")
	seg.End()

	seg = txn.StartSegment("HashingNotification")
	hash := xxhash.Sum64(data)
	log(ctx, notification, zerolog.DebugLevel, "notification hashed")
	seg.End()

	result, err := datastore.Client.SAdd(ctx, datastore.JobIdSet, hash).Result()
	if err != nil {
		msg := fmt.Sprintf("ERROR ADDING %v TO SET: %v", hash, err)
		log(ctx, notification, zerolog.ErrorLevel, msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	if result == 0 {
		msg := "Notification payload not unique!"
		log(ctx, notification, zerolog.WarnLevel, msg)
		return nil, status.Errorf(codes.AlreadyExists, msg)
	}
	log(ctx, notification, zerolog.DebugLevel, "notification hash added to set")

	key := txn.GetTraceMetadata().TraceID
	_, err = datastore.Client.Set(ctx, key, data, 0).Result()
	if err != nil {
		msg := fmt.Sprintf("ERROR SETTING KEY %v TO PAYLOAD: %v", key, err)
		log(ctx, notification, zerolog.ErrorLevel, msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	log(ctx, notification, zerolog.DebugLevel, fmt.Sprintf("key %v set to payload", key))

	log(ctx, notification, zerolog.DebugLevel, fmt.Sprintf("adding key %v to jobs queue", key))
	_, err = datastore.Client.RPush(ctx, datastore.JobsQueue, key).Result()
	if err != nil {
		msg := fmt.Sprintf("ERROR ADDING %v TO JOBS QUEUE: %v", key, err)
		log(ctx, notification, zerolog.ErrorLevel, msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	defer log(ctx, notification, zerolog.InfoLevel, fmt.Sprintf("Notification response returned. SendMessage call duration: %v", time.Since(start)))
	return &pb.NotificationResponse{Message: "Message Received"}, nil
}

func (s *healthCheckServer) Check(ctx context.Context, req *pbh.HealthCheckRequest) (*pbh.HealthCheckResponse, error) {
	return &pbh.HealthCheckResponse{Message: "Alive"}, nil
}

func AuthInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	telemetry.LogWithContext(zerolog.DebugLevel, "auth intercept", ctx)

	token := extractFromContext(ctx)
	if !isTokenValid(token) {
		telemetry.LogWithContext(zerolog.WarnLevel, "invalid auth token", ctx)
		return nil, status.Errorf(codes.Unauthenticated, "token invalid")
	}
	return handler(ctx, req)
}

func log(ctx context.Context, notification *pb.NotificationPackage, level zerolog.Level, msg string) {
	logger := telemetry.NewLogger(ctx)
	logger.WithLevel(level).
		Str("title", notification.Title).
		Str("body", notification.Body).
		Str("path", notification.Data["path"]).
		Str("contentId", notification.Data["contentId"]).
		Str("contentType", notification.Data["contentType"]).
		Msg(telemetry.MsgWithTraceID(ctx, msg))
}

func extractFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	tokens := md.Get("Authorization")
	if len(tokens) == 0 {
		return ""
	}
	return tokens[0]
}

func isTokenValid(token string) bool {
	authed := os.Getenv("AUTHED")
	return token == authed
}
